import akka.actor._
import akka.agent._

case class Propose(client: ActorRef, value: Any)

case class Prepare(n: Long)
case class Promise(from: ActorRef)
case class Preempted(n: Long)

case class Accept(n: Long, value: Any)
case class Accepted(from: ActorRef)

case class Apply(n: Long, value: Any)

class PaxosMidget(n: Long, value: Any, membership: Set[ActorRef]) extends Actor {
  var awaiting = membership

  awaiting.foreach(_ ! Prepare(n))

  def phase1: PartialFunction[Any, Unit] = {
    case Promise(from) =>
      awaiting = awaiting - from
      println("got promise from " + from)
      if (awaiting.size < membership.size/2) {
        println("time to transition")
        awaiting = membership
        awaiting.foreach(_ ! Accept(n, value))
        context.become(phase2)
      }
  }

  def phase2: PartialFunction[Any, Unit] = {
    case Accepted(from) =>
      awaiting = awaiting - from
      println("got accepted from " + from)
      if (awaiting.size < membership.size/2) {
        membership.foreach(_ ! Apply(n, value))
        context.stop(self)
      }
  }

  override def preStart() {
    context.become(phase1)
  }

  def receive = { case _ => println("I shouldn't be here") }
}

class Leader(membership: Agent[Set[ActorRef]]) extends Actor {
  var seq = 1L

  def receive = {
    case Propose(client, value) =>
      context.actorOf(Props(new PaxosMidget(seq, value, membership())))
      seq += 1
  }

}

class Acceptor extends Actor {
  var myN: Long = 0

  def receive = {
    case Prepare(n) if myN < n =>
      myN = n
      sender ! Promise(context.parent)
    case Prepare(n) =>
      println("Ignoring out of date Prepare("+n+")")
    case Accept(n, _) if n <= myN =>
      sender ! Accepted(context.parent)
    case Accept(n, v) =>
      println("Ignoring out of date Accept("+n+","+v+")")
  }

}

class Replica extends Actor {
  def receive = {
    case any => println("got " + any)
  }
}

class PaxosSup(membership: Agent[Set[ActorRef]]) extends Actor {
  val leader = context.actorOf(Props(new Leader(membership)))
  val acceptor = context.actorOf(Props[Acceptor])
  val replica = context.actorOf(Props[Replica])

  def receive = {
    case p: Propose => 
      leader forward p
    case p: Prepare =>
      acceptor forward p
    case a: Accept =>
      acceptor forward a
    case a: Apply =>
      replica forward a
  }
}

implicit val as = ActorSystem("paxosbeta")

val membership = Agent[Set[ActorRef]](Set[ActorRef]())
val p1 = as.actorOf(Props(new PaxosSup(membership)))
val p2 = as.actorOf(Props(new PaxosSup(membership)))
val p3 = as.actorOf(Props(new PaxosSup(membership)))

membership send (_ + p1)
membership send (_ + p2)
membership send (_ + p3)
