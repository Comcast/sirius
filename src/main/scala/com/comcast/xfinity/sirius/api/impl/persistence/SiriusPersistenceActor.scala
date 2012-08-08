package com.comcast.xfinity.sirius.api.impl.persistence

import akka.actor.Actor
import akka.actor.ActorRef
import com.comcast.xfinity.sirius.writeaheadlog.SiriusLog
import com.comcast.xfinity.sirius.api.SiriusResult
import akka.agent.Agent
import com.comcast.xfinity.sirius.api.impl._
import akka.event.Logging
import collection.SortedSet
import collection.immutable.TreeSet
import annotation.tailrec

/**
 * {@link Actor} for persisting data to the write ahead log and forwarding
 * to the state worker.
 */
class SiriusPersistenceActor(val stateActor: ActorRef, siriusLog: SiriusLog, siriusStateAgent: Agent[SiriusState])
  extends Actor {

  val eventOrdering = Ordering.fromLessThan[OrderedEvent](_.sequence < _.sequence)
  var orderedEventBuffer = TreeSet.empty[OrderedEvent](eventOrdering)

  val logger = Logging(context.system, this)

  override def preStart() {

    logger.info("Bootstrapping Write Ahead Log")
    val start = System.currentTimeMillis()
    // XXX: replace accum Unit with Any? then we don't have to worry
    //      about returning a unit in the end
    siriusLog.foldLeft(Unit)((acc, orderedEvent) => {
      stateActor ! orderedEvent.request
      acc
    })
    logger.info("Done Bootstrapping Write Ahead Log in {} ms", System.currentTimeMillis() - start)
    siriusStateAgent send ((state: SiriusState) => {
      state.updatePersistenceState(SiriusState.PersistenceState.Initialized)
    })

  }

  def receive = {
    case event: OrderedEvent => {
      stateActor forward event.request

      val nextSeq = siriusLog.getNextSeq
      if (event.sequence == nextSeq) {
        // siriusLog.currentSeq gets incremented with this write
        siriusLog.writeEntry(event)

        // write events and drop from buffer if they match seq num
        orderedEventBuffer = dropUntil(orderedEventBuffer)((oe: OrderedEvent) => {
            if (oe.sequence == siriusLog.getNextSeq) {
              siriusLog.writeEntry(oe)
              false
            } else {
              true
            }
        }).asInstanceOf[TreeSet[OrderedEvent]]
      } else if (event.sequence > nextSeq) {
        orderedEventBuffer += event
      } else {
        logger.debug("Ignored out-of-order event: "+event+"; expected sequence number >= "+nextSeq)
      }
    }
    case _: SiriusResult =>
  }

  /**
   * For use with sorted sets, drops items from the head UNTIL pred returns true
   */
  @tailrec
  final def dropUntil[A](set: SortedSet[A])(pred: (A) => Boolean): SortedSet[A] = {
    set.headOption match {
      case Some(x) if (!pred(x)) => dropUntil(set.tail)(pred)
      case _ => set
    }
  }
}