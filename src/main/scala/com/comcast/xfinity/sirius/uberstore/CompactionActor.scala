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
package com.comcast.xfinity.sirius.uberstore

import akka.actor.{Props, Actor}
import com.comcast.xfinity.sirius.writeaheadlog.SiriusLog
import com.comcast.xfinity.sirius.uberstore.CompactionManager.Compact

object CompactionActor {
  case object CompactionComplete
  case class CompactionFailed(exception: Exception)

  /**
   * Create Props for CompactionActor, actor responsible for calling into SiriusLog's compact.
   * The liveness of this actor can be used to determine whether compaction is ongoing.
   * Accepts no messages other than Compact.
   *
   * @param siriusLog log to compact
   */
  def props(siriusLog: SiriusLog): Props = {
    Props(classOf[CompactionActor], siriusLog)
  }
}

class CompactionActor(siriusLog: SiriusLog) extends Actor {
  import CompactionActor._

  def receive = {
    case Compact =>
      try {
        siriusLog.compact()
        sender ! CompactionComplete

      } catch {
        case e: Exception =>
          sender ! CompactionFailed(e)

      } finally {
        context.stop(self)
      }
  }
}
