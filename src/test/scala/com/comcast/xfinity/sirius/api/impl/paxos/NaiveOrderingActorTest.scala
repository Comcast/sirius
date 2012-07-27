package com.comcast.xfinity.sirius.api.impl.paxos

import akka.util.duration._

import org.junit.Assert.assertTrue


import akka.testkit.TestActorRef
import akka.testkit.TestProbe
import com.comcast.xfinity.sirius.NiceTest
import com.comcast.xfinity.sirius.api.impl.membership._
import org.mockito.Mockito._
import akka.agent.Agent
import com.comcast.xfinity.sirius.info.SiriusInfo
import com.comcast.xfinity.sirius.api.impl._
import akka.actor.{ActorRef, ActorSystem}

class NaiveOrderingActorTest extends NiceTest {

  var actorSystem: ActorSystem = _
  var underTestActor: TestActorRef[NaiveOrderingActor] = _
  var underTest: NaiveOrderingActor = _
  var persistenceProbe: TestProbe = _


  def createProbedNaiveOrderingActor(persistenceProbe: TestProbe)(implicit as: ActorSystem) = {
    TestActorRef(new NaiveOrderingActor(persistenceProbe.ref))

  }

  before {
    actorSystem = ActorSystem("testsystem")
    persistenceProbe = TestProbe()(actorSystem)


    underTestActor = createProbedNaiveOrderingActor(persistenceProbe)(actorSystem)
    underTest = underTestActor.underlyingActor
  }

  after {
    actorSystem.shutdown()
  }

  describe("a NaiveOrderingActor") {
    it("should forward Put's to the persistence actor") {
      val put = Put("key", "body".getBytes)
      underTestActor ! put
      val OrderedEvent(_, _, actualPut) = persistenceProbe.receiveOne(5 seconds)
      assert(put == actualPut)
    }

    it("should increase its internal counter on Put's") {
      val origCount = underTest.seq
      underTestActor ! Put("key", "body".getBytes)
      val OrderedEvent(finalCount, _, _) = persistenceProbe.receiveOne(5 seconds)
      assertTrue(origCount < finalCount)
    }

    it("should forward Delete's to the persistence actor") {
      val del = Delete("key")
      underTestActor ! del
      val OrderedEvent(_, _, actualDel) = persistenceProbe.receiveOne(5 seconds)
      assert(del == actualDel)
    }

    it("should increase its internal counter on Delete's") {
      val origCount = underTest.seq
      underTestActor ! Delete("key")
      val OrderedEvent(finalCount, _, _) = persistenceProbe.receiveOne(5 seconds)
      assertTrue(origCount < finalCount)
    }

  }
}