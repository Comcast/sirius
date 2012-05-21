package com.comcast.xfinity.sirius.api.impl.paxos
import com.comcast.xfinity.sirius.api.impl.Delete
import com.comcast.xfinity.sirius.api.impl.Put

import akka.actor.Actor
import akka.actor.ActorRef

/**
 * Actor for negotiating Paxos rounds locally.
 * 
 * For now this Actor only accepts {@link Put} and {@link Get}
 * messages. Behaviour for other messages is undefined.
 * 
 * TODO: Currently this is just a placeholder for the Paxos layer,
 *       here to draw a cutoff point
 */
class SiriusPaxosActor(val persistenceActor: ActorRef) extends Actor {

  var seq: Long = 0L
  
  def receive = {
    case put: Put => 
      seq += 1
      persistenceActor forward put
    case del: Delete => 
      seq += 1
      persistenceActor forward del
  }
  
}