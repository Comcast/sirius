package com.comcast.xfinity.sirius.api.impl.paxos
import akka.actor.Actor
import com.comcast.xfinity.sirius.api.impl.paxos.PaxosMessages._

class Acceptor extends Actor {
  var ballotNum: Ballot = Ballot.empty
  var accepted = Map[Long, PValue]()

  // Note that Phase1A and Phase2B requests must return their
  // parent's address (their PaxosSup) because this is how
  // the acceptor is known to external nodes
  def receive = {
    // Scout
    case Phase1A(scout, ballot) =>
      if (ballot > ballotNum) ballotNum = ballot
      scout ! Phase1B(context.parent, ballotNum, accepted.values.toSet)

    // Commander
    case Phase2A(commander, pval) =>
      if (pval.ballot >= ballotNum) {
        ballotNum = pval.ballot
        accepted += (accepted.get(pval.slotNum) match {
          case Some(oldPval) if oldPval.ballot > pval.ballot => (oldPval.slotNum -> oldPval)
          case _ => (pval.slotNum -> pval)
        })
      }
      commander ! Phase2B(context.parent, ballotNum)
  }
}