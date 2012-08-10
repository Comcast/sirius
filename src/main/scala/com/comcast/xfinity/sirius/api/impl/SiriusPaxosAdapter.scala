package com.comcast.xfinity.sirius.api.impl

import akka.actor.{Props, ActorRef}
import akka.agent.Agent
import paxos.PaxosMessages.{Command, Decision, RequestPerformed}
import paxos.{Replica, PaxosSup}
import annotation.tailrec
import collection.SortedMap

/**
 * Class responsible for adapting the Paxos subsystem for use in Sirius
 *
 * This class contains the necessary logic for assuring that events are only
 * applied to the persistence layer in order.  As designed currently (on
 * purpose) the Paxos system will blindly deliver decisions, even if they have
 * already been decided.  This allows nodes that are behind to catch up.  Also,
 * there is no gaurentee that events will arrive in order, so a later event
 * may arrive before a current event.
 *
 * To accomplish this we buffer events that come before their time, only keeping
 * the first copy of each.
 *
 * The paxosSubSystemProps member Props factory should be used to instantiate
 * the Paxos subsystem. A sample usage would be:
 *
 *    val paxosAdapter = new SiriusPaxosAdapter(membership, 1, persistenceActor)
 *    context.actorOf(paxosAdapter.paxosSubsystemProps)
 *
 * @param membership the Agent[Set[ActorRef]] identifying cluster members
 * @param startingSeq the sequnce number to start with
 * @param persistenceActor the actor to send commited events to. This actor must
 *          know how to receive and understand
 */
class SiriusPaxosAdapter(membership: Agent[Set[ActorRef]],
                         startingSeq: Long,
                         persistenceActor: ActorRef) {

  var nextSeq: Long = startingSeq
  var eventBuffer = SortedMap[Long, OrderedEvent]()

  /**
   * Function used to apply updates to the persistence layer. When a decision arrives
   * for the first time the actor identified by Decision.command.client is sent a
   * RequestPerformed message (a little misleading, I know). The decision is then
   * buffered to be sent to the persistence layer.  All ready decisions (contiguous
   * ones starting with the current sequence number) are sent to the persistence
   * layer to be written to disk and memory, and are dropped from the buffer.
   *
   * XXX: in its current form it does not wait for confirmation that an event has been
   * committed to disk by the persistence layer, we should really add that, but for
   * now this should be good enough.
   */
  val performFun: Replica.PerformFun = {
    case Decision(slot, Command(client, ts, op)) if slot >= nextSeq && !eventBuffer.contains(slot) =>
      eventBuffer += (slot -> OrderedEvent(slot, ts, op))
      client ! RequestPerformed
      executeReadyDecisions()
    case _ => // no-op, ignore that jawn
  }

  /**
   * Props factory that should be used to instantiate the subsystem
   */
  val paxosSubsystemProps = Props(PaxosSup(membership, startingSeq, performFun))

  private def executeReadyDecisions() {
    @tailrec
    def applyNextReadyDecision() {
      eventBuffer.headOption match {
        case Some((slot, orderedEvent)) if slot == nextSeq =>
          persistenceActor ! orderedEvent
          nextSeq += 1
          eventBuffer = eventBuffer.tail
          applyNextReadyDecision()
        case _ =>
      }
    }
    applyNextReadyDecision()
  }
}