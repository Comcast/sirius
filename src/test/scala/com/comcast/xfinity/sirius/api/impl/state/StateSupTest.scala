package com.comcast.xfinity.sirius.api.impl.state

import org.scalatest.BeforeAndAfterAll
import com.comcast.xfinity.sirius.NiceTest
import akka.testkit.{TestActorRef, TestProbe}
import com.comcast.xfinity.sirius.api.impl.state.SiriusPersistenceActor._
import com.comcast.xfinity.sirius.writeaheadlog.SiriusLog
import com.comcast.xfinity.sirius.api.impl.{SiriusState, OrderedEvent, Delete, Get}
import akka.agent.Agent
import org.mockito.Mockito._
import org.mockito.Matchers._
import com.comcast.xfinity.sirius.api.{SiriusConfiguration, RequestHandler}
import akka.actor.{ActorContext, ActorRef, ActorSystem}

object StateSupTest {
  def makeMockedUpChildProvider(implicit actorSystem: ActorSystem): (TestProbe, TestProbe, StateSup.ChildProvider) = {
    val stateProbe = TestProbe()
    val persistenceProbe = TestProbe()
    val provider = new StateSup.ChildProvider(null, null, null) {
      override def createStateActor()(implicit context: ActorContext): ActorRef = stateProbe.ref
      override def createPersistenceActor(stateActor: ActorRef)(implicit context: ActorContext): ActorRef = persistenceProbe.ref
    }
    (stateProbe, persistenceProbe, provider)
  }
}

class StateSupTest extends NiceTest with BeforeAndAfterAll {

  import StateSupTest._

  implicit val actorSystem = ActorSystem("StateSupTest")

  override def afterAll {
    actorSystem.shutdown()
  }

  describe("when receiving a Get") {
    it ("must forward the message to the in memory state subsystem") {
      val mockRequestHandler = mock[RequestHandler]
      val mockLog = mock[SiriusLog]
      val mockStateAgent = mock[Agent[SiriusState]]

      val (stateProbe, _, mockChildProvider) = makeMockedUpChildProvider

      val stateSup = TestActorRef(new StateSup(mockRequestHandler, mockLog, mockStateAgent, mockChildProvider))

      val senderProbe = TestProbe()
      senderProbe.send(stateSup, Get("asdf"))
      stateProbe.expectMsg(Get("asdf"))
      assert(senderProbe.ref === stateProbe.lastSender)
    }
  }

  describe("when receiving an OrderedEvent") {
    it ("must send the OrderedEvent to the persistence subsystem") {
      val mockRequestHandler = mock[RequestHandler]
      val mockLog = mock[SiriusLog]
      val mockStateAgent = mock[Agent[SiriusState]]

      val (_, persistenceProbe, mockChildProvider) = makeMockedUpChildProvider

      val stateSup = TestActorRef(new StateSup(mockRequestHandler, mockLog, mockStateAgent, mockChildProvider))

      val orderedEvent = OrderedEvent(1, 1, Delete("asdf"))
      stateSup ! orderedEvent
      persistenceProbe.expectMsg(orderedEvent)
      assert(stateSup === persistenceProbe.lastSender)
    }
  }

  describe("when receiving a LogQuery message") {
    it ("must forward the message to the persistence subsystem") {
      val mockRequestHandler = mock[RequestHandler]
      val mockLog = mock[SiriusLog]
      val mockStateAgent = mock[Agent[SiriusState]]

      val (_, persistenceProbe, mockChildProvider) = makeMockedUpChildProvider

      val stateSup = TestActorRef(new StateSup(mockRequestHandler, mockLog, mockStateAgent, mockChildProvider))

      val senderProbe = TestProbe()
      senderProbe.send(stateSup, GetLogSubrange(1, 100))
      persistenceProbe.expectMsg(GetLogSubrange(1, 100))
      assert(senderProbe.ref === persistenceProbe.lastSender)

      senderProbe.send(stateSup, GetNextLogSeq)
      persistenceProbe.expectMsg(GetNextLogSeq)
      assert(senderProbe.ref === persistenceProbe.lastSender)
    }
  }
}