package com.comcast.xfinity.sirius.api.impl.persistence

import org.scalatest.BeforeAndAfter
import org.scalatest.FunSpec
import akka.actor.ActorSystem
import akka.testkit.TestActorRef
import akka.testkit.TestProbe
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import com.comcast.xfinity.sirius.writeaheadlog.{LogData, LogWriter}


import org.mockito.Mockito._
import org.mockito.ArgumentCaptor
import akka.util.duration._
import com.comcast.xfinity.sirius.api.impl.{OrderedEvent, Put, Delete}

@RunWith(classOf[JUnitRunner])
class SiriusPersistenceActorTest extends FunSpec with BeforeAndAfter with MockitoSugar {

  var actorSystem: ActorSystem = _

  var underTestActor: TestActorRef[SiriusPersistenceActor] = _
  var testStateWorkerProbe: TestProbe = _

  var mockLogWriter: LogWriter = _

  before {
    mockLogWriter = mock[LogWriter]

    actorSystem = ActorSystem("testsystem")

    testStateWorkerProbe = TestProbe()(actorSystem)
    underTestActor = TestActorRef(new SiriusPersistenceActor(testStateWorkerProbe.ref, mockLogWriter))(actorSystem)

  }

  after {
    actorSystem.shutdown()
  }

  //actionType: String, key: String, sequence: Long, timestamp: Long, payload: Option[Array[Byte]]
  describe("a SiriusPersistenceActor") {
    it("should forward Put's to the state actor") {
      val put = Put("key", "body".getBytes)
      val event = OrderedEvent(0L, 0L, put)
      underTestActor ! event
      val actualPut = testStateWorkerProbe.receiveOne(5 seconds)
      assert(put === actualPut)

      val argument = ArgumentCaptor.forClass(classOf[LogData])
      verify(mockLogWriter, times(1)).writeEntry(argument.capture())
      val logData = argument.getValue()
      assert("PUT" === logData.actionType)
    }

    it("should forward Delete's to the state actor") {
      val delete = Delete("key")
      val event = OrderedEvent(0L, 0L, delete)
      underTestActor ! event
      val actualDelete = testStateWorkerProbe.receiveOne(5 seconds)
      assert(delete === actualDelete)

      val argument = ArgumentCaptor.forClass(classOf[LogData])
      verify(mockLogWriter, times(1)).writeEntry(argument.capture())
      val logData = argument.getValue()
      assert("DELETE" === logData.actionType)
    }
  }

}