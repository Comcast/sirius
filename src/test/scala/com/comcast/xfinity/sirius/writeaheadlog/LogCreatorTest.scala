package com.comcast.xfinity.sirius.api.impl
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.BeforeAndAfter
import org.scalatest.FunSpec

import com.comcast.xfinity.sirius.writeaheadlog.LogCreator
import com.comcast.xfinity.sirius.writeaheadlog.LogData

@RunWith(classOf[JUnitRunner])
class LogCreatorTest extends FunSpec with BeforeAndAfter {
  var logCreator : LogCreator = _
  
  before {
    logCreator = new LogCreator()
  }
  
  describe("A Sirius write ahead log") {
    it("Returns a string representation of the log contents") {
      val logData = new LogData("PUT", "key", 123L, 12345L , Array[Byte](65))
      val expectedLogEntry = "PUT|key|123|19691231T190012.345-0500|QQ=="
        
      assertLogEntriesEqual(expectedLogEntry, logCreator.createLogEntry(logData))
    }
  }
  
  def assertLogEntriesEqual(expected : String, underTest : String) = {
    val Array(expectedActionType, expectedKey, expectedSequence, expectedTimestamp, expectedPayload) = expected.split("\\|")
    val Array(testActionType, testKey, testSequence, testTimestamp, testPayload) = underTest.split("\\|")
    
    assert(testActionType === expectedActionType)
    assert(testKey === expectedKey)
    assert(testSequence === expectedSequence)
    assert(testTimestamp === expectedTimestamp)
    assert(testPayload === expectedPayload)
  }
}