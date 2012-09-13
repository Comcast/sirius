package com.comcast.xfinity.sirius.api.impl

import compat.AkkaFutureAdapter
import com.comcast.xfinity.sirius.api.RequestHandler
import com.comcast.xfinity.sirius.api.Sirius
import akka.pattern.ask
import membership._
import com.comcast.xfinity.sirius.api.SiriusResult
import akka.actor._
import java.util.concurrent.Future
import com.comcast.xfinity.sirius.writeaheadlog.SiriusLog
import com.comcast.xfinity.sirius.api.SiriusConfiguration
import akka.dispatch.{Await, Future => AkkaFuture}
import akka.util.Timeout
import akka.util.duration._

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
    val supProps = Props(SiriusSupervisor(requestHandler, siriusLog, config))
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
  implicit val timeout: Timeout =
    (config.getProp(SiriusConfiguration.CLIENT_TIMEOUT_MS, 5000) milliseconds)

  private[impl] var onShutdownHook: Option[(() => Unit)] = None

  val supervisor = actorSystem.actorOf(supProps, supName)

  def getMembership = {
    val akkaFuture = (supervisor ? GetMembershipData).asInstanceOf[AkkaFuture[Set[ActorRef]]]
    new AkkaFutureAdapter(akkaFuture)
  }

  /**
   * Returns true if the underlying Sirius is up and ready to handle requests
   *
   * @return true if system is ready, false if not
   */
  def isOnline: Boolean = !supervisor.isTerminated && askIfInitialized(supervisor)

  def checkClusterConfig() {
    supervisor ! CheckClusterConfig
  }

  /**
   * ${@inheritDoc}
   */
  def enqueueGet(key: String): Future[SiriusResult] = {
    val akkaFuture = (supervisor ? Get(key)).asInstanceOf[AkkaFuture[SiriusResult]]
    new AkkaFutureAdapter(akkaFuture)
  }

  /**
   * ${@inheritDoc}
   */
  def enqueuePut(key: String, body: Array[Byte]): Future[SiriusResult] = {
    //XXX: this will always return a Sirius.None as soon as Ordering is complete
    val akkaFuture = (supervisor ? Put(key, body)).asInstanceOf[AkkaFuture[SiriusResult]]
    new AkkaFutureAdapter(akkaFuture)
  }

  /**
   * ${@inheritDoc}
   */
  def enqueueDelete(key: String): Future[SiriusResult] = {
    val akkaFuture = (supervisor ? Delete(key)).asInstanceOf[AkkaFuture[SiriusResult]]
    new AkkaFutureAdapter(akkaFuture)
  }

  /**
   * Set a block of code to run on shutDown
   *
   * @param shutdownHook call by name code to execute
   */
  def onShutdown(shutdownHook: => Unit) {
    onShutdownHook = Some(() => shutdownHook)
  }

  /**
   * Terminate this instance.  Shuts down all associated Actors.
   */
  def shutdown() {
    supervisor ! Kill
    onShutdownHook match {
      case Some(shutdownHook) => shutdownHook()
      case None => //do nothing
    }
  }


  private def askIfInitialized(supRef: ActorRef): Boolean = {
    val isInitializedFuture =
        (supRef ? SiriusSupervisor.IsInitializedRequest).mapTo[SiriusSupervisor.IsInitializedResponse]
      Await.result(isInitializedFuture, timeout.duration).initialized
  }
}
