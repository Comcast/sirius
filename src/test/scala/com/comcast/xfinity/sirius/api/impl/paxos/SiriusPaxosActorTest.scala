package com.comcast.xfinity.sirius.api.impl.paxos

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.BeforeAndAfter
import org.scalatest.FunSpec

import akka.util.duration._

import org.junit.Assert.assertTrue


import akka.actor.ActorSystem
import akka.testkit.TestActorRef
import akka.testkit.TestProbe
import com.comcast.xfinity.sirius.api.impl.{OrderedEvent, Delete, Put}

@RunWith(classOf[JUnitRunner])
class SiriusPaxosActorTest extends FunSpec with BeforeAndAfter {

  var actorSystem: ActorSystem = _
  var underTestActor: TestActorRef[SiriusPaxosActor] = _
  var underTest: SiriusPaxosActor = _
  var testPersistenceActorProbe: TestProbe = _

  before {
    actorSystem = ActorSystem("testsystem")
    testPersistenceActorProbe = TestProbe()(actorSystem)
    underTestActor = TestActorRef(new SiriusPaxosActor(testPersistenceActorProbe.ref))(actorSystem)
    underTest = underTestActor.underlyingActor
  }

  after {
    actorSystem.shutdown()
  }

  describe("a SiriusPaxosActor") {
    it("should forward Put's to the persistence actor") {
      val put = Put("key", "body".getBytes)
      underTestActor ! put
      val OrderedEvent(_, _, actualPut) = testPersistenceActorProbe.receiveOne(5 seconds)
      assert(put == actualPut)
    }

    it("should increase its internal counter on Put's") {
      val origCount = underTest.seq
      underTestActor ! Put("key", "body".getBytes)
      val OrderedEvent(finalCount, _, _) = testPersistenceActorProbe.receiveOne(5 seconds)
      assertTrue(origCount < finalCount)
    }

    it("should forward Delete's to the persistence actor") {
      val del = Delete("key")
      underTestActor ! del
      val OrderedEvent(_, _, actualDel) = testPersistenceActorProbe.receiveOne(5 seconds)
      assert(del == actualDel)
    }

    it("should increase its internal counter on Delete's") {
      val origCount = underTest.seq
      underTestActor ! Delete("key")
      val OrderedEvent(finalCount, _, _) = testPersistenceActorProbe.receiveOne(5 seconds)
      assertTrue(origCount < finalCount)
    }

  }
}