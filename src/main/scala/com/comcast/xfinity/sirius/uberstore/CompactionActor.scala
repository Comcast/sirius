package com.comcast.xfinity.sirius.uberstore

import akka.actor.Actor
import com.comcast.xfinity.sirius.writeaheadlog.SiriusLog
import com.comcast.xfinity.sirius.uberstore.CompactionManager.Compact

object CompactionActor {
  case object CompactionComplete
  case class CompactionFailed(exception: Exception)
}

/**
 * Actor responsible for calling into SiriusLog's compact. The liveness of this actor
 * can be used to determine whether compaction is ongoing. Accepts no messages other than
 * Compact.
 *
 * @param siriusLog log to compact
 */
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
