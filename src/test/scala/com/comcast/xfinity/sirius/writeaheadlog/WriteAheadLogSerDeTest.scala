package com.comcast.xfinity.sirius.writeaheadlog

import com.comcast.xfinity.sirius.NiceTest

class WriteAheadLogSerDeTest extends NiceTest {
  
  var logEntry: WriteAheadLogSerDe = _

  before {
    logEntry = new WriteAheadLogSerDe()

  }

  describe("A Sirius write ahead log entry") {
    it("should serialize and deserialize to the same thing") {
      val rawLogEntry = "ZXnHgnjaTQHEEwNVOo7wuw==|PUT|key|123|19700101T000012.345Z|QQ==\n"
      assert(rawLogEntry === logEntry.serialize(logEntry.deserialize(rawLogEntry)))
    }

    it("should throw exception if never was deserialized") {
      intercept[NullPointerException] {
        logEntry.serialize(null)
      }
    }

    it("should throw a SiriusChecksumException when deserializing a tampered with log entry") {
      val rawLogEntry = "ZXnHgnjaTQHEEwNVOo7wuw==|PUT|tamperedkey|123|19700101T000012.345Z|QQ==\n"

      intercept[SiriusChecksumException] {
        logEntry.deserialize(rawLogEntry)

      }
    }

    it("should serialize to a string representation of the log contents.") {
      val logData = new LogData("PUT", "key", 123L, 12345L, Some(Array[Byte](65)))
      val expectedLogEntry = "ZXnHgnjaTQHEEwNVOo7wuw==|PUT|key|123|19700101T000012.345Z|QQ==\n"
      assert(expectedLogEntry == logEntry.serialize(logData))
    }

    it("should properly encodes payloads that have a | character when serializing.") {
      val logData = new LogData("PUT", "key", 123L, 12345L, Some(Array[Byte](65, 124, 65)))
      val expectedLogEntry = "FcKBMsXg++2Z44UoYNnmSA==|PUT|key|123|19700101T000012.345Z|QXxB\n"
      assert(expectedLogEntry == logEntry.serialize(logData))
    }

    it("should throw an exception when attempting to use a key with a | character when serializing.") {
      val logData = new LogData("PUT", "key|foo|bar", 123L, 12345L, Some(Array[Byte](65)))
      intercept[IllegalStateException] {
        logEntry.serialize(logData)
      }
    }

    // TODO: Figure out the right way to test that we're not allowing any whitespace
    // using FunSpec w/o a bunch of cut and paste.
    it("should throw an exception when attempting to use a key with Unicode U+0020 in it, when serializing.") {
      val logData = new LogData("PUT", "key foo bar", 123L, 12345L, Some(Array[Byte](65)))
      intercept[IllegalStateException] {
        logEntry.serialize(logData)
      }
    }

    it("should throw an exception when attempting to use a key with Unicode U+000A in it when serializing.") {
      val logData = new LogData("PUT", "key\nfoo\nbar", 123L, 12345L, Some(Array[Byte](65)))
      intercept[IllegalStateException] {
        logEntry.serialize(logData)
      }
    }
  }
}