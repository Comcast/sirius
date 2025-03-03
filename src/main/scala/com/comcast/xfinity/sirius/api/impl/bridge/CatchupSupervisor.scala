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

import akka.actor.{Actor, ActorRef, Props}
import akka.pattern.ask

import scala.concurrent.duration._
import com.comcast.xfinity.sirius.api.impl.membership.MembershipHelper
import com.comcast.xfinity.sirius.api.SiriusConfiguration
import com.comcast.xfinity.sirius.api.impl.bridge.CatchupSupervisor.CatchupSupervisorInfoMBean
import com.comcast.xfinity.sirius.api.impl.state.SiriusPersistenceActor.{CompleteSubrange, GetLogSubrange, GetLogSubrangeWithLimit, LogSubrange}
import com.comcast.xfinity.sirius.admin.MonitoringHooks

import scala.util.Success

case class InitiateCatchup(fromSeq: Long)
case class ContinueCatchup(fromSeq: Long)
case object StopCatchup

case object CatchupRequestFailed
case class CatchupRequestSucceeded(logSubrange: LogSubrange)

object CatchupSupervisor {

  trait CatchupSupervisorInfoMBean {
    def getSSThresh: Int
    def getLimit: Int
  }

  /**
   * Build Props for a new CatchupSupervisor.
   *
   * @param membershipHelper MembershipHelper for finding non-local cluster members for catchup.
   * @param config SiriusConfiguration for the configs
   * @return Props object for a new CatchupSupervisor
   */
  def props(membershipHelper: MembershipHelper, config: SiriusConfiguration): Props = {
    val timeoutCoeff = config.getDouble(SiriusConfiguration.CATCHUP_TIMEOUT_INCREASE_PER_EVENT, .01)
    val timeoutConst = config.getDouble(SiriusConfiguration.CATCHUP_TIMEOUT_BASE, 1.0)
    val maxWindowSize = config.getInt(SiriusConfiguration.CATCHUP_MAX_WINDOW_SIZE, 1000)
    val maxLimitSize = if (config.getProp(SiriusConfiguration.CATCHUP_USE_LIMIT, false)) {
      config.getProp[Int](SiriusConfiguration.CATCHUP_MAX_LIMIT_SIZE)
    } else {
      None
    }
    // must ensure ssthresh <= maxWindowSize
    val startingSSThresh = Math.min(maxLimitSize.getOrElse(maxWindowSize), config.getInt(SiriusConfiguration.CATCHUP_DEFAULT_SSTHRESH, 500))
    Props(new CatchupSupervisor(membershipHelper, timeoutCoeff, timeoutConst, maxWindowSize, maxLimitSize, startingSSThresh, config))
  }
}

/**
 * Long-living supervisor for the catchup process.
 *
 * The catchup process uses a variation on the Slow Start / Congestion Avoidance tactics from TCP
 * Tahoe. See http://en.wikipedia.org/wiki/TCP_congestion-avoidance_algorithm#TCP_Tahoe_and_Reno
 *
 * This actor requests a series of events, the number of which is determined by the window. Catchup begins
 * in Slow Start phase, and the window is initialized to 1. With each successful request, the window doubles,
 * until it reaches ssthresh. At this point, catchup enters Congestion Avoidance phase, where successful
 * requests add 2 to the window size, until maxWindowSize is met, or there is an error.
 *
 * At any point, if there is a timeout in requesting a log range:
 * - ssthresh becomes failure_window_size / 2
 * - window is reset to 1
 * - catchup reenters Slow Start phase
 */
private[bridge] class CatchupSupervisor(membershipHelper: MembershipHelper,
                                        timeoutCoeff: Double,
                                        timeoutConst: Double,
                                        maxWindowSize: Int,
                                        maxLimitSize: Option[Int],
                                        var ssthresh: Int,
                                        config: SiriusConfiguration) extends Actor with MonitoringHooks {

  var limit = 1
  def timeout() = (timeoutConst + (limit * timeoutCoeff)).seconds

  implicit val executionContext = context.dispatcher

  def receive = {
    case InitiateCatchup(fromSeq) =>
      membershipHelper.getRandomMember.map(remote => {
        requestSubrange(fromSeq, limit, remote)
        context.become(catchup(remote))
      })
  }

  def catchup(source: ActorRef): Receive = {
    case CatchupRequestSucceeded(logSubrange: CompleteSubrange) =>
      if (limit >= ssthresh) { // we're in Congestion Avoidance phase
        limit = Math.min(limit + 2, maxLimitSize.getOrElse(maxWindowSize))
      } else { // we're in Slow Start phase
        limit = Math.min(limit * 2, ssthresh)
      }
      context.parent ! logSubrange

    case CatchupRequestSucceeded(logSubrange) =>
      context.parent ! logSubrange

    case CatchupRequestFailed =>
      if (limit != 1) {
        // adjust ssthresh, revert to Slow Start phase
        ssthresh = Math.max(limit / 2, 1)
        limit = 1
      }
      context.unbecome()

    case ContinueCatchup(fromSeq: Long) =>
      requestSubrange(fromSeq, limit, source)

    case StopCatchup =>
      context.unbecome()
  }

  def requestSubrange(fromSeq: Long, limit: Int, source: ActorRef): Unit = {
    val message = maxLimitSize match {
      case Some(_) => GetLogSubrangeWithLimit(fromSeq, fromSeq + maxWindowSize, limit)
      case None => GetLogSubrange(fromSeq, fromSeq + limit)
    }
    source.ask(message)(timeout()).onComplete {
      case Success(logSubrange: LogSubrange) => self ! CatchupRequestSucceeded(logSubrange)
      case _ => self ! CatchupRequestFailed
    }
  }

  override def preStart(): Unit = {
    registerMonitor(new CatchupSupervisorInfo(), config)
  }
  override def postStop(): Unit = {
    unregisterMonitors(config)
  }

  class CatchupSupervisorInfo extends CatchupSupervisorInfoMBean {
    def getSSThresh = ssthresh
    def getLimit = limit
  }
}
