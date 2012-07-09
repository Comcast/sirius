import com.comcast.xfinity.sirius.api.impl.paxos._
import PaxosMessages._
import akka.agent._
import akka.actor._

implicit val as = ActorSystem("test")
val membership = Agent(Set[ActorRef]())

val p1 = PaxosSup("node1", membership)
val p2 = PaxosSup("node2", membership)
val p3 = PaxosSup("node3", membership)

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

