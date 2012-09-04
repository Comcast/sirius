package com.comcast.xfinity.sirius.api.impl.state

import org.scalatest.BeforeAndAfterAll
import com.comcast.xfinity.sirius.NiceTest
import akka.actor.ActorSystem
import akka.testkit.{TestActorRef, TestProbe}
import com.comcast.xfinity.sirius.api.impl.{OrderedEvent, Delete, Get}
import com.comcast.xfinity.sirius.api.impl.state.SiriusPersistenceActor.GetLogSubrange

class StateSupTest extends NiceTest with BeforeAndAfterAll {

  implicit val actorSystem = ActorSystem("StateSupTest")

  override def afterAll {
    actorSystem.shutdown()
  }

  describe("when receiving a Get") {
    it ("must forward the message to the in memory state subsystem") {
      val stateProbe = TestProbe()
      val stateSup = TestActorRef(new StateSup with StateSup.ChildProvider {
        val stateActor = stateProbe.ref
        val persistenceActor = TestProbe().ref
      })

      val senderProbe = TestProbe()
      senderProbe.send(stateSup, Get("asdf"))
      stateProbe.expectMsg(Get("asdf"))
      assert(senderProbe.ref === stateProbe.lastSender)
    }
  }

  describe("when receiving an OrderedEvent") {
    it ("must send the OrderedEvent to the persistence subsystem") {
      val persistenceProbe = TestProbe()
      val stateSup = TestActorRef(new StateSup with StateSup.ChildProvider {
        val stateActor = TestProbe().ref
        val persistenceActor = persistenceProbe.ref
      })

      val orderedEvent = OrderedEvent(1, 1, Delete("asdf"))
      stateSup ! orderedEvent
      persistenceProbe.expectMsg(orderedEvent)
      assert(stateSup === persistenceProbe.lastSender)
    }
  }

  describe("when receiving a GetLogSubRange message") {
    it ("must forward the message to the persistence subsystem") {
      val persistenceProbe = TestProbe()
      val stateSup = TestActorRef(new StateSup with StateSup.ChildProvider {
        val stateActor = TestProbe().ref
        val persistenceActor = persistenceProbe.ref
      })

      val senderProbe = TestProbe()
      senderProbe.send(stateSup, GetLogSubrange(1, 100))
      persistenceProbe.expectMsg(GetLogSubrange(1, 100))
      assert(senderProbe.ref === persistenceProbe.lastSender)
    }
  }
}