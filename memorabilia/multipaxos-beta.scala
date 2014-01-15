import akka.actor._

case class Command(k: ActorRef, cid: Int, op: Any => (Any, Any))

class Replica(leaders: Set[ActorRef], initialState: Any) extends Actor {
  var state = initialState
  var slotNum = 1
  
  var proposals = Set[(Int, Command)]()
  var decisions = Set[(Int, Command)]()

  def propose(p: Command) = {
    if (!proposals.exists({ case (_, `p`) => true; case _ => false})) {
      val s = (proposals ++ decisions).foldLeft(1)((m, t) => if (m > t._1) m else t._1) + 1
      proposals = proposals + Tuple2(s, p)
      leaders.foreach(_ ! ('propose, s, p))
    }
  }

  def perform(p: Command) = {
    if (decisions.exists({ case (s, `p`) if s < slotNum => true; case _ => false })) {
      slotNum += 1
    } else {
      val (next, result) = p.op(state)
      state = next
      slotNum += 1
      p.k ! ('response, p.cid, result)
    }
  }

  def receive = {
    case ('request, p: Command) => propose(p)
    case ('decision, s: Int, p: Command) =>
      decisions += Tuple2(s, p)
      def doWhile(): Unit = decisions.find({ case (sS, _) if sS == slotNum=> true; case _ => false }) match {
        case Some((_, pP)) => 
          proposals.find({ case (sS, _) if sS == slotNum=> true; case _ => false }) match {
            case Some((_, pPP)) if pP != pPP => propose(pPP)
            case None =>
          }
          perform(pP)
          doWhile()
        case None =>
      }
      doWhile()
  }
}

case class Ballot(seq: Int, leaderId: String) extends Ordered[Ballot] {
  def compare(that: Ballot) = that match {
    case Ballot(thatSeq, _) if seq < thatSeq => -1
    case Ballot(thatSeq, _) if seq > thatSeq => 1
    case Ballot(_, thatLeaderId) if leaderId < thatLeaderId => -1
    case Ballot(_, thatLeaderId) if leaderId > thatLeaderId => 1
    case _ => 0
  }
}

class Acceptor extends Actor {
  var ballotNum: Ballot = Ballot(Int.MinValue, "")
  var accepted = Set[(Ballot, Int, Command)]()

  def receive = {
    case ('p1a, leader: ActorRef, b: Ballot) =>
      if (b > ballotNum) ballotNum = b
      leader ! ('p1b, self, ballotNum, accepted)
    case ('p2a, leader: ActorRef, (b: Ballot, s: Int, p: Command)) =>
      if (b >= ballotNum) {
        ballotNum = b
        accepted += Tuple3(b, s, p)
      }
      leader ! ('p2b, self, ballotNum)
  }
}

class Commander(leader: ActorRef, acceptors: Set[ActorRef], replicas: Set[ActorRef], pval: (Ballot, Int, Command)) extends Actor {
  val (b, s, p) = pval

  var waitFor = acceptors

  acceptors.foreach(_ ! ('p2a, self, pval))

  def receive = {
    case ('p2a, acceptor: ActorRef, bB: Ballot) =>
      if (b == bB) {
        waitFor -= acceptor
        if (waitFor.size < acceptors.size/2) {
          replicas.foreach(_ ! ('decision, s, p))
          context.stop(self)
        }
      } else {
        leader ! ('preempted, bB)
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
        if (waitFor.size < acceptors.size/2) {
          leader ! ('adopted, b, pvalues)
          context.stop(self)
        }
      } else {
        leader ! ('preempted, bB)
        context.stop(self)
      }
  }
}

class Leader(acceptors: Set[ActorRef], replicas: Set[ActorRef]) extends Actor {
  var ballotNum = Ballot(0, self.toString)
  var active = false
  var proposals = Set[(Int, Command)]()

  context.actorOf(Props(new Scout(self, acceptors, ballotNum)))

  def receive = {
    case ('propose, s: Int, p: Command) =>
      if (!proposals.exists({ case (sS, _) if sS == s => true; case _ => false })) {
        proposals += Tuple2(s, p)
        if (active) {
          context.actorOf(Props(new Commander(self, acceptors, replicas, (ballotNum, s, p))))
        }
      }
    case ('adopted, ballotNum: Ballot, pvals: Set[(Ballot, Int, Command)]) =>
      proposals = update(proposals, pmax(pvals))
      proposals.foreach({ case (s, p) => context.actorOf(Props(new Commander(self, acceptors, replicas, (ballotNum, s, p)))) })
      active = true
    case ('preempted, bB: Ballot) =>
      if (bB > ballotNum) {
        active = false
        ballotNum = Ballot(bB.seq + 1, self.toString)
        context.actorOf(Props(new Scout(self, acceptors, ballotNum)))
      }
  }


  def update(x: Set[(Int, Command)], y: Set[(Int, Command)]) = y ++ x.filterNot(y.contains(_))
 

  def pmax(pvals: Set[(Ballot, Int, Command)]) = {
    pvals.foldLeft(Map[Int, (Ballot, Int, Command)]())({
      case (acc, (b, s, p)) => acc.get(s) match {
        case None => acc + (s -> (b, s, p))
        case Some((bB, sS, pP)) if b >= bB => acc + (s -> (b, s, p))
        case _ => acc
      }
    }).foldLeft(Set[(Int, Command)]()) {
      case (acc, (_, (b, s, p))) => acc + Tuple2(s, p)
    }
  }

}
