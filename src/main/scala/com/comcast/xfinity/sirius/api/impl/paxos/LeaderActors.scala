package com.comcast.xfinity.sirius.api.impl.paxos
import akka.actor._
import com.comcast.xfinity.sirius.api.impl.paxos.PaxosMessages._

class LeaderActors {

  class Leader(acceptors: Set[ActorRef], replicas: Set[ActorRef]) extends Actor {
    var ballotNum = Ballot(0, self.toString)
    var active = false
    var proposals = Set[(Int, Command)]()

    context.actorOf(Props(new Scout(self, acceptors, ballotNum)))

    def receive = {
      case Propose(slot: Int, command: Command) =>
        if (!proposals.exists({ case (sS, _) if sS == slot => true; case _ => false })) {
          proposals += Tuple2(slot, command)
          if (active) {
            context.actorOf(Props(new Commander(self, acceptors, replicas, (ballotNum, slot, command))))
          }
        }
      case Adopted(ballotNum: Ballot, pvals: Set[PValue]) =>
        proposals = update(proposals, pmax(pvals))
        proposals.foreach({ case (s, p) => context.actorOf(Props(new Commander(self, acceptors, replicas, (ballotNum, s, p)))) })
        active = true
      case Preempted(picked: Ballot) =>
        if (picked > ballotNum) {
          active = false
          ballotNum = Ballot(picked.seq + 1, self.toString)
          context.actorOf(Props(new Scout(self, acceptors, ballotNum)))
        }
    }

    def update(x: Set[(Int, Command)], y: Set[(Int, Command)]) = y ++ x.filterNot(y.contains(_))

    def pmax(pvals: Set[PValue]) : Set[Tuple2[Int, Command]] = {
      pvals.foldLeft(Map[Int, PValue]())({
        case (acc, PValue(b, s, p)) => acc.get(s) match {
          case None => acc + (s -> PValue(b, s, p))
          case Some(PValue(bB, sS, pP)) if b >= bB => acc + (s -> PValue(b, s, p))
          case _ => acc
        }
      }).foldLeft(Set[(Int, Command)]()) {
        case (acc, (_, PValue(b, s, p))) => acc + Tuple2(s, p)
      }
    }
  }

  class Commander(leader: ActorRef, acceptors: Set[ActorRef], replicas: Set[ActorRef], pval: (Ballot, Int, Command)) extends Actor {
    val (b, s, p) = pval

    var waitFor = acceptors

    acceptors.foreach(_ ! ('p2a, self, pval))

    def receive = {
      case Phase2B(acceptor, ballot) =>
        if (b == ballot) {
          waitFor -= acceptor
          if (waitFor.size < acceptors.size / 2) {
            replicas.foreach(_ ! ('decision, s, p))
            context.stop(self)
          }
        } else {
          leader ! ('preempted, ballot)
        }
    }
  }

  class Scout(leader: ActorRef, acceptors: Set[ActorRef], b: Ballot) extends Actor {
    var waitFor = acceptors
    var pvalues = Set[(Ballot, Int, Command)]()

    acceptors.foreach(_ ! ('p1a, self, b))

    def receive = {
      case ('p1b, acceptor: ActorRef, bB: Ballot, r: Set[(Ballot, Int, Command)]) =>
        if (bB == b) {
          pvalues ++= r
          waitFor -= acceptor
          if (waitFor.size < acceptors.size / 2) {
            leader ! ('adopted, b, pvalues)
            context.stop(self)
          }
        } else {
          leader ! ('preempted, bB)
          context.stop(self)
        }
    }
  }
}