/*
 *  Copyright 2012-2014 Comcast Cable Communications Management, LLC
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.comcast.xfinity.sirius.api.impl

import akka.actor._
import akka.pattern.ask
import akka.util.Timeout
import com.comcast.xfinity.sirius.api.{RequestHandler, Sirius, SiriusConfiguration, SiriusResult}
import com.comcast.xfinity.sirius.api.impl.compat.AkkaFutureAdapter
import com.comcast.xfinity.sirius.api.impl.membership.MembershipActor._
import com.comcast.xfinity.sirius.api.impl.status.NodeStats.FullNodeStatus
import com.comcast.xfinity.sirius.api.impl.status.StatusWorker._
import com.comcast.xfinity.sirius.writeaheadlog.SiriusLog

import java.util.concurrent.CompletableFuture
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future => AkkaFuture}
import scala.util.Try

object SiriusImpl {

  /**
   * Create a new SiriusImpl.
   *
   * @param requestHandler the RequestHandler containing the callbacks for manipulating this instance's state
   * @param siriusLog the log to be used for persisting events
   * @param config SiriusConfiguration object full of all kinds of configuration goodies, see SiriusConfiguration
   *            for more information
   */
  def apply(requestHandler: RequestHandler,
            siriusLog: SiriusLog,
            config: SiriusConfiguration)
           (implicit actorSystem: ActorSystem): SiriusImpl = {
    val supProps = SiriusSupervisor.props(requestHandler, siriusLog, config)
    new SiriusImpl(config, supProps)
  }
}

/**
 * Create a SiriusImpl
 *
 * This is a semi-internal API, you should prefer the companion object's apply.
 *
 * @param config SiriusConfiguration object full of all kinds of configuration goodies, see SiriusConfiguration
 *            for more information
 * @param supProps Props factory for creating the supervisor
 * @param actorSystem the actorSystem to use to create the Actors for Sirius
 */
class SiriusImpl(config: SiriusConfiguration, supProps: Props)(implicit val actorSystem: ActorSystem)
    extends Sirius {

  val supName = config.getProp(SiriusConfiguration.SIRIUS_SUPERVISOR_NAME, "sirius")
  implicit val timeout: Timeout = {
    config.getProp(SiriusConfiguration.CLIENT_TIMEOUT_MS, 5000).milliseconds
  }
  implicit val ec: ExecutionContext = ExecutionContext.global

  private[impl] var onShutdownHook: Option[(() => Unit)] = None
  private var isTerminated = false

  val supervisor = actorSystem.actorOf(supProps, supName)

  def getMembership: CompletableFuture[Map[String, Option[ActorRef]]] = {
    val akkaFuture = (supervisor ? GetMembershipData).asInstanceOf[AkkaFuture[Map[String, Option[ActorRef]]]]
    new AkkaFutureAdapter(akkaFuture)
  }

  /**
   * Returns true if the underlying Sirius is up and ready to handle requests
   *
   * @return true if system is ready, false if not
   */
  def isOnline: Boolean = !isTerminated && askIfInitialized(supervisor)

  def checkClusterConfig(): Unit = {
    supervisor ! CheckClusterConfig
  }

  /**
   * @inheritdoc
   */
  def enqueueGet(key: String): CompletableFuture[SiriusResult] = {
    val akkaFuture = (supervisor ? Get(key)).asInstanceOf[AkkaFuture[SiriusResult]]
    new AkkaFutureAdapter(akkaFuture)
  }

  /**
   * @inheritdoc
   */
  def enqueuePut(key: String, body: Array[Byte]): CompletableFuture[SiriusResult] = {
    //XXX: this will always return a Sirius.None as soon as Ordering is complete
    val akkaFuture = (supervisor ? Put(key, body)).asInstanceOf[AkkaFuture[SiriusResult]]
    new AkkaFutureAdapter(akkaFuture)
  }

  /**
   * @inheritdoc
   */
  def enqueueDelete(key: String): CompletableFuture[SiriusResult] = {
    val akkaFuture = (supervisor ? Delete(key)).asInstanceOf[AkkaFuture[SiriusResult]]
    new AkkaFutureAdapter(akkaFuture)
  }

  /**
   * Get this nodes status, included in the result are the nodes address,
   * configuration, and value of any monitors, if configured
   */
  def getStatus: CompletableFuture[FullNodeStatus] = {
    val akkaFuture = (supervisor ? GetStatus).asInstanceOf[AkkaFuture[FullNodeStatus]]
    new AkkaFutureAdapter(akkaFuture)
  }

  /**
   * Set a block of code to run on shutDown
   *
   * @param shutdownHook call by name code to execute
   */
  def onShutdown(shutdownHook: => Unit): Unit = {
    onShutdownHook = Some(() => shutdownHook)
  }

  /**
   * Terminate this instance.  Shuts down all associated Actors.
   */
  def shutdown(): Unit = {
    actorSystem.stop(supervisor)
    isTerminated = true
    onShutdownHook match {
      case Some(shutdownHook) => shutdownHook()
      case None => //do nothing
    }
  }

  private def askIfInitialized(supRef: ActorRef): Boolean = {
    val isInitializedFuture =
        (supRef ? SiriusSupervisor.IsInitializedRequest).mapTo[SiriusSupervisor.IsInitializedResponse]
    Try(Await.result(isInitializedFuture, timeout.duration).initialized).getOrElse(false)
  }
}
