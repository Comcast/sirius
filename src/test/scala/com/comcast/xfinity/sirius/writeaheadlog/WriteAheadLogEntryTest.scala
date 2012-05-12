package com.comcast.xfinity.sirius.writeaheadlog

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.BeforeAndAfter
import org.scalatest.FunSpec
import java.security.MessageDigest
import org.apache.commons.codec.binary.Base64
import org.mockito.Mockito._
import org.mockito.Matchers._

@RunWith(classOf[JUnitRunner])
class WriteAheadLogEntryTest extends FunSpec with BeforeAndAfter {
  var logEntry: WriteAheadLogEntry = _
  var mockCodec: Base64 = _
  var mockMessageDigest: MessageDigest = _

  before {
    logEntry = new WriteAheadLogEntry()

  }

  describe("A Sirius write ahead log entry") {
    it("should serialize and deserialize to the same thing") {
      val rawLogEntry = "PUT|key|123|19700101T000012.345Z|QQ==|Q6UvDVS1BZtbrPcdyUwakQ==\r"
      logEntry.deserialize(rawLogEntry)
      assert(rawLogEntry === logEntry.serialize())
    }

    it("should throw exception if never was deserialized") {
      intercept[IllegalStateException] {
        logEntry.serialize()
      }
    }

    it("should throw a SiriusChecksumException when deserializing a tampered with log entry") {
      val rawLogEntry = "PUT|tamperedkey|123|19700101T000012.345Z|QQ==|Q6UvDVS1BZtbrPcdyUwakQ==\r"

      intercept[SiriusChecksumException] {
        logEntry.deserialize(rawLogEntry)

      }
    }

    it("should serialize to a string representation of the log contents.") {
      val logData = new LogData("PUT", "key", 123L, 12345L, Array[Byte](65))
      logEntry.logData = logData;
      val expectedLogEntry = "PUT|key|123|19700101T000012.345Z|QQ==|Q6UvDVS1BZtbrPcdyUwakQ==\r"
      assertLogEntriesEqual(expectedLogEntry, logEntry.serialize())
    }

    it("should properly encodes payloads that have a | character when serializing.") {
      val logData = new LogData("PUT", "key", 123L, 12345L, Array[Byte](65, 124, 65))
      logEntry.logData = logData;
      val expectedLogEntry = "PUT|key|123|19700101T000012.345Z|QXxB|Uw81FQiQ3WGGglvYtWG0ew==\r"
      assertLogEntriesEqual(expectedLogEntry, logEntry.serialize())
    }

    it("should throw an exception when attempting to use a key with a | character when serializing.") {
      val logData = new LogData("PUT", "key|foo|bar", 123L, 12345L, Array[Byte](65))
      logEntry.logData = logData
      intercept[IllegalStateException] {
        logEntry.serialize()
      }
    }

    // TODO: Figure out the right way to test that we're not allowing any whitespace
    // using FunSpec w/o a bunch of cut and paste.
    it("should throw an exception when attempting to use a key with Unicode U+0020 in it, when serializing.") {
      val logData = new LogData("PUT", "key foo bar", 123L, 12345L, Array[Byte](65))
      logEntry.logData = logData
      intercept[IllegalStateException] {
        logEntry.serialize()
      }
    }

    it("should throw an exception when attempting to use a key with Unicode U+000A in it when serializing.") {
      val logData = new LogData("PUT", "key\nfoo\nbar", 123L, 12345L, Array[Byte](65))
      logEntry.logData = logData
      intercept[IllegalStateException] {
        logEntry.serialize()
      }
    }

  }

  def assertLogEntriesEqual(expected: String, underTest: String) = {
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