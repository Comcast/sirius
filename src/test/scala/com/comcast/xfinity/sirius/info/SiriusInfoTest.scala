package com.comcast.xfinity.sirius.info

import com.comcast.xfinity.sirius.NiceTest
import org.mockito.Mockito._
import akka.testkit.{TestActor, TestProbe}
import akka.actor.{ActorRef, ActorSystem}
import com.comcast.xfinity.sirius.api.impl.membership
import membership._


import com.comcast.xfinity.sirius.api.impl.paxos.PaxosMessages._
import org.apache.commons.lang.builder.ReflectionToStringBuilder

class SiriusInfoTest extends NiceTest {

  var siriusInfo: SiriusInfo = _
  var system: ActorSystem = _
  var probe: TestProbe = _

  before {
    system = ActorSystem("yo")
    probe = new TestProbe(system)
    siriusInfo = new SiriusInfo(4242, "foobar", probe.ref)

  }

  after {
    system.shutdown()
    system.awaitTermination()
  }

  describe("a SiriusInfo") {
    it("returns a name when getName is called") {
      assert("sirius-foobar:4242" == siriusInfo.getName)
    }
    it("should return getname when toString is called") {
      assert(siriusInfo.getName === siriusInfo.toString())
    }
    it("should return membership") {
      probe.setAutoPilot(new TestActor.AutoPilot {
        def run(sender: ActorRef, msg: Any): Option[TestActor.AutoPilot] = msg match {
          case GetMembershipData => {
            sender ! Set[ActorRef]()
            Some(this)
          }
        }
      })
      assert(ReflectionToStringBuilder.toString(Set[ActorRef]()) === siriusInfo.getMembership)
    }
    it("should return latest slot") {
      probe.setAutoPilot(new TestActor.AutoPilot {
        def run(sender: ActorRef, msg: Any): Option[TestActor.AutoPilot] = msg match {
          case GetLowestUnusedSlotNum => {
            sender ! LowestUnusedSlotNum(1L)
            Some(this)
          }
        }
      })
      assert("1" === siriusInfo.getLatestSlot)
    }
  }
}