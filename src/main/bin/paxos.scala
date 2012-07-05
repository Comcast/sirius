import com.comcast.xfinity.sirius.api.impl.paxos._
import PaxosMessages._
import akka.agent._
import akka.actor._

implicit val as = ActorSystem("test")
val membership = Agent(Set[ActorRef]())

val p1 = as.actorOf(Props(PaxosSup(membership)), "node1")
val p2 = as.actorOf(Props(PaxosSup(membership)), "node2")
val p3 = as.actorOf(Props(PaxosSup(membership)), "node3")

membership.send(_ + p1)
membership.send(_ + p2)
membership.send(_ + p3)

class PrintingActor extends Actor {
  def receive = {
    case any => println(self + " received: " + any)
  }
}

val dummy = as.actorOf(Props[PrintingActor])

val req = Request(Command(dummy, 1234, 1))

