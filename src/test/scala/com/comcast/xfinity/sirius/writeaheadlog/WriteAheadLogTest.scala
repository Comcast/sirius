package com.comcast.xfinity.sirius.api.impl
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.BeforeAndAfter
import org.scalatest.FunSpec

import com.comcast.xfinity.sirius.writeaheadlog.WriteAheadLog
import com.comcast.xfinity.sirius.writeaheadlog.LogData

@RunWith(classOf[JUnitRunner])
class LogCreatorTest extends FunSpec with BeforeAndAfter {
  var logCreator : WriteAheadLog = _
  
  before {
    logCreator = new WriteAheadLog()
  }
  
  describe("A Sirius write ahead log") {
    it("Returns a string representation of the log contents.") {
      val logData = new LogData("PUT", "key", 123L, 12345L , Array[Byte](65))
      val expectedLogEntry = "PUT|key|123|19691231T190012.345-0500|QQ==|NGdbieMz1OKbWsuEa9ZBGQ==\r"

      assertLogEntriesEqual(expectedLogEntry, logCreator.createLogEntry(logData))
    }
    
    it("Properly encodes payloads that have a | character.") {
      val logData = new LogData("PUT", "key", 123L, 12345L, Array[Byte](65, 124, 65))
      val expectedLogEntry = "PUT|key|123|19691231T190012.345-0500|QXxB|t2h71DyaSgBSgS2xrEf3FQ==\r"
        
      assertLogEntriesEqual(expectedLogEntry, logCreator.createLogEntry(logData))
    }
    
    it("Throws an exception when attempting to use a key with a | character.") {
      val logData = new LogData("PUT", "key|foo|bar", 123L, 12345L, Array[Byte](65))
      intercept[IllegalStateException] {
          logCreator.createLogEntry(logData)
      }
    }
    
    // TODO: Figure out the right way to test that we're not allowing any whitespace
    // using FunSpec w/o a bunch of cut and paste.
    it("Throws an exception when attempting to use a key with Unicode U+0020 in it.") {
      val logData = new LogData("PUT", "key foo bar", 123L, 12345L, Array[Byte](65))
      intercept[IllegalStateException] {
          logCreator.createLogEntry(logData)
      }
    }
    
    it("Throws an exception when attempting to use a key with Unicode U+000A in it.") {
      val logData = new LogData("PUT", "key\nfoo\nbar", 123L, 12345L, Array[Byte](65))
      intercept[IllegalStateException] {
          logCreator.createLogEntry(logData)
      }
    }
    
  }
  
  def assertLogEntriesEqual(expected : String, underTest : String) = {
    val Array(expectedActionType, expectedKey, expectedSequence, expectedTimestamp, expectedPayload, expectedChecksum) = expected.split("\\|")
    val Array(testActionType, testKey, testSequence, testTimestamp, testPayload, testChecksum) = underTest.split("\\|")
    
    assert(testActionType === expectedActionType)
    assert(testKey === expectedKey)
    assert(testSequence === expectedSequence)
    assert(testTimestamp === expectedTimestamp)
    assert(testPayload === expectedPayload)
    assert(testChecksum == expectedChecksum)
  }
}