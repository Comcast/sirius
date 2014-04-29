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
package com.comcast.xfinity.sirius.api.impl.bridge

import akka.actor.{ActorContext, ActorRef, Props, Actor}
import scala.concurrent.duration._
import com.comcast.xfinity.sirius.api.impl.membership.MembershipHelper
import com.comcast.xfinity.sirius.api.SiriusConfiguration
import com.comcast.xfinity.sirius.api.impl.bridge.CatchupSupervisor.{CatchupSupervisorInfoMBean, ChildProvider}
import com.comcast.xfinity.sirius.api.impl.state.SiriusPersistenceActor.CompleteSubrange
import com.comcast.xfinity.sirius.admin.MonitoringHooks

case class InitiateCatchup(fromSeq: Long)
case class ContinueCatchup(fromSeq: Long)
case object StopCatchup

object CatchupSupervisor {
  class ChildProvider(config: SiriusConfiguration) {
    def createCatchupActor(source: ActorRef, seq: Long, window: Int, timeout: FiniteDuration)(implicit context: ActorContext): ActorRef = {
      context.actorOf(CatchupActor.props(seq, window, timeout, source))
    }
  }

  trait CatchupSupervisorInfoMBean {
    def getSSThresh: Int
    def getWindow: Int
  }

  /**
   * Build Props for a new CatchupSupervisor.
   *
   * @param membershipHelper MembershipHelper for finding non-local cluster members for catchup.
   * @param config SiriusConfiguration for the configs
   * @return Props object for a new CatchupSupervisor
   */
  def props(membershipHelper: MembershipHelper, config: SiriusConfiguration): Props = {
    val childProvider = new ChildProvider(config)
    val timeoutCoeff = config.getProp(SiriusConfiguration.CATCHUP_TIMEOUT_INCREASE_PER_EVENT, ".01").toDouble
    val timeoutConst = config.getProp(SiriusConfiguration.CATCHUP_TIMEOUT_BASE, "1").toDouble
    Props(classOf[CatchupSupervisor], childProvider, membershipHelper, timeoutCoeff, timeoutConst, config)
  }
}

/**
 * Long-living supervisor for the catchup process. Holds window/ssthresh state and coordinates
 * CatchupActors.
 */
private[bridge] class CatchupSupervisor(childProvider: ChildProvider,
                                        membershipHelper: MembershipHelper,
                                        timeoutCoeff: Double,
                                        timeoutConst: Double,
                                        config: SiriusConfiguration) extends Actor with MonitoringHooks {

  val maxWindowSize = 1000
  var ssthresh = 500
  var window = 1
  def timeout() = (timeoutConst + (window * timeoutCoeff)).seconds

  implicit val executionContext = context.dispatcher

  def receive = {
    case InitiateCatchup(fromSeq) =>
      membershipHelper.getRandomMember.map(remote => {
        childProvider.createCatchupActor(remote, fromSeq, window, timeout())
        context.become(catchup(remote))
      })
  }

  def catchup(source: ActorRef): Receive = {
    case CatchupRequestSucceeded(logSubrange: CompleteSubrange) =>
      // XXX could use another .become to differentiate between SS / CA, but this isn't too complicated
      if (window >= ssthresh) { // CA phase
        window = Math.min(window + 2, maxWindowSize)
      }  else { // SS phase
        window = Math.min(window * 2, ssthresh)
      }
      context.parent ! logSubrange

    case CatchupRequestSucceeded(logSubrange) =>
      context.parent ! logSubrange

    case CatchupRequestFailed =>
      if (window != 1) {
        ssthresh = Math.max(window / 2, 1)
        window = 1
      }
      context.unbecome()

    case ContinueCatchup(fromSeq: Long) =>
      childProvider.createCatchupActor(source, fromSeq, window, timeout())

    case StopCatchup =>
      context.unbecome()
  }

  override def preStart() {
    registerMonitor(new CatchupSupervisorInfo(), config)
  }
  override def postStop() {
    unregisterMonitors(config)
  }

  class CatchupSupervisorInfo extends CatchupSupervisorInfoMBean {
    def getSSThresh = ssthresh
    def getWindow = window
  }
}
