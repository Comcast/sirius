package com.comcast.xfinity.sirius.api.impl.persistence
import org.scalatest.BeforeAndAfter
import org.scalatest.FunSpec
import akka.actor.ActorSystem
import akka.testkit.TestActorRef
import akka.testkit.TestProbe
import com.comcast.xfinity.sirius.api.impl.Put
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import com.comcast.xfinity.sirius.api.impl.Delete
import com.comcast.xfinity.sirius.api.impl.Get

@RunWith(classOf[JUnitRunner])
class SiriusPersistenceActorTest extends FunSpec with BeforeAndAfter {

  var actorSystem: ActorSystem = _
  
  var underTestActor: TestActorRef[SiriusPersistenceActor] = _
  var testStateWorkerProbe: TestProbe = _
  
  before {
    actorSystem = ActorSystem("testsystem")
    
    testStateWorkerProbe = TestProbe()(actorSystem)
    underTestActor = TestActorRef(new SiriusPersistenceActor(testStateWorkerProbe.ref))(actorSystem)
  }
  
  after {
    actorSystem.shutdown()
  }
  
  describe("a SiriusPersistenceActor") {
    it("should forward Put's to the state actor") {
      val put = Put("key", "body".getBytes)
      underTestActor ! put
      testStateWorkerProbe.expectMsg(put)
    }
    
    it("should forward Delete's to the state actor") {
      val delete = Delete("key")
      underTestActor ! delete
      testStateWorkerProbe.expectMsg(delete)
    }
    
    it("does not handle Get's") {
      val get = Get("key")
      underTestActor ! get
      testStateWorkerProbe.expectNoMsg()
    }
  }
  
}