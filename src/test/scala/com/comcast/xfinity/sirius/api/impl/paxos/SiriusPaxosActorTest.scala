package com.comcast.xfinity.sirius.api.impl.paxos
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.BeforeAndAfter
import org.scalatest.FunSpec

import org.junit.Assert.assertTrue

import com.comcast.xfinity.sirius.api.impl.Delete
import com.comcast.xfinity.sirius.api.impl.Put

import akka.actor.ActorSystem
import akka.testkit.TestActorRef
import akka.testkit.TestProbe

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
      testPersistenceActorProbe.expectMsg(put)
    }
    
    it("should increase it's internal counter on Put's") {
      val origCount = underTest.seq
      underTestActor ! Put("key", "body".getBytes)
      val finalCount = underTest.seq
      assertTrue(origCount < finalCount)
    }
    
    it("should forward Delete's to the persistence actor") { 
      val del = Delete("key")
      underTestActor ! del
      testPersistenceActorProbe.expectMsg(del)
    }
    
    it("should increase it's internal counter on Delete's") {
      val origCount = underTest.seq
      underTestActor ! Delete("key")
      val finalCount = underTest.seq
      assertTrue(origCount < finalCount)
    }
    
  }
}