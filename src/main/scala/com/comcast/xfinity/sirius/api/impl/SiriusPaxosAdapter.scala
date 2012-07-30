package com.comcast.xfinity.sirius.api.impl

import akka.actor.{Props, ActorRef}
import akka.agent.Agent
import paxos.{Replica, PaxosSup}
import akka.dispatch.Await
import akka.pattern.ask

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
 * To accomplish this we ignore any events which do not match the next expected
 * sequence number.
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
                         persistenceActor: ActorRef) extends AkkaConfig{

  var currentSeq: Long = startingSeq

  /**
   * Function used to apply updates to the persistence layer, updates will only
   * be applied in order.  If a decision arrives not matching the current sequence
   * number it is ignored, with the expectation that it will eventually arrive again,
   * and false is returned, signaling that the client SHOULD NOT be replied to.
   *
   * If a decision does match we send it to the persistence layer, as an OrderedEvent,
   * update the next expected sequence number, and return true, signaling that the
   * client SHOULD be returned to.
   *
   * XXX: in its current form it does not wait for confirmation that an event has been
   * committed to disk by the persistence layer, we should really add that, but for
   * now this should be good enough.
   *
   * XXX: should we consider having the context passed into here somehow so that we
   * can use forward?  While this may run on an actor thread, to use forward we
   * need an implicit ActorContext.
   */
  val performFun: Replica.PerformFun =
    (slotNum: Long, req: NonCommutativeSiriusRequest) => {
      // only apply the event if it is for the sequence number we were expecting,
      // otherwise ignore it. If it's above the expected sequence number odds are
      // it will get delivered again, if it's under then we don't care either way
      if (slotNum == currentSeq) {
        val persistedFuture = persistenceActor ? OrderedEvent(slotNum, System.currentTimeMillis(), req)
        val result = Await.result(persistedFuture, timeout.duration)

        if (result == true) {
          currentSeq += 1
          true
        } else {
          false
        }
      } else {
        false
      }
    }

  /**
   * Props factory that should be used to instantiate the subsystem
   */
  val paxosSubsystemProps = Props(PaxosSup(membership, startingSeq, performFun))
}