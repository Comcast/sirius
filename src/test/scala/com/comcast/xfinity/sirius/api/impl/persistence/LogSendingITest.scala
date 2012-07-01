package com.comcast.xfinity.sirius.api.impl.persistence

import com.comcast.xfinity.sirius.NiceTest
import akka.actor.ActorSystem
import org.scalatest.BeforeAndAfterAll
import akka.testkit.{TestActorRef, TestFSMRef}
import org.mockito.Mockito._
import com.comcast.xfinity.sirius.writeaheadlog.LogLinesSource

class LogSendingITest extends NiceTest with BeforeAndAfterAll {

  implicit val actorSystem = ActorSystem("actorSystem")

  var sender : TestFSMRef[LSState, LSData, LogSendingActor] = _
  var receiver : TestActorRef[LogReceivingActor] = _
  var mockSource : LogLinesSource = _

  before {
    sender = TestFSMRef(new LogSendingActor)
    receiver = TestActorRef(new LogReceivingActor)
    mockSource = mock[LogLinesSource]
  }

  override def afterAll() {
    actorSystem.shutdown()
  }

  describe("Log sender communicatation with log receiver") {
    it("should execute on a very simple input without failure") {
      when(mockSource.createLinesIterator()).thenReturn(Iterator("a", "b", "c", "d", "e"))
      sender ! Start(receiver, mockSource, 2)
      // TODO when we're doing more than printing the output, add more meaningful test to ensure it arrives successfully
    }
  }


}
