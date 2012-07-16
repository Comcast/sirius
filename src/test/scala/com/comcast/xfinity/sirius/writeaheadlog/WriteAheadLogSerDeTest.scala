package com.comcast.xfinity.sirius.writeaheadlog

import com.comcast.xfinity.sirius.NiceTest
import com.comcast.xfinity.sirius.api.impl.{OrderedEvent, Put}

class WriteAheadLogSerDeTest extends NiceTest {
  
  var logEntry: WriteAheadLogSerDe = _

  before {
    logEntry = new WriteAheadLogSerDe()
  }

  describe("A Sirius write ahead log entry") {
    it("should serialize and deserialize to the same thing") {
      val rawLogEntry = "38a3d11c36c4c4e1|PUT|key|123|19700101T000012.345Z|QQ==\n"
      assert(rawLogEntry === logEntry.serialize(logEntry.deserialize(rawLogEntry)))
    }

    it("should throw exception if the input is illogical") {
      intercept[NullPointerException] {
        logEntry.serialize(null)
      }
    }

    it("should throw a SiriusChecksumException when deserializing a tampered with log entry") {
      val rawLogEntry = "38a3d11c36c4c4e1|PUT|tamperedkey|123|19700101T000012.345Z|QQ==\n"

      intercept[SiriusChecksumException] {
        logEntry.deserialize(rawLogEntry)

      }
    }

    it("should serialize to a string representation of the log contents.") {
      val event = OrderedEvent(123L, 12345L, Put("key", "A".getBytes))
      val expectedLogEntry = "38a3d11c36c4c4e1|PUT|key|123|19700101T000012.345Z|QQ==\n"
      assert(expectedLogEntry == logEntry.serialize(event))
    }

    it("should properly encodes payloads that have a | character when serializing.") {
      val event = OrderedEvent(123L, 12345L, Put("key", "A|A".getBytes))
      val expectedLogEntry = "8e8ca658d0c63868|PUT|key|123|19700101T000012.345Z|QXxB\n"
      assert(expectedLogEntry == logEntry.serialize(event))
    }

    it("should throw an exception when attempting to use a key with a | character when serializing.") {
      intercept[IllegalStateException] {
        val event = OrderedEvent(123L, 12345L, Put("key|foo|bar", "A".getBytes))
        logEntry.serialize(event)
      }
    }

    // TODO: Figure out the right way to test that we're not allowing any whitespace
    // using FunSpec w/o a bunch of cut and paste.
    it("should throw an exception when attempting to use a key with Unicode U+0020 in it, when serializing.") {
      intercept[IllegalStateException] {
        val event = OrderedEvent(123L, 12345L, Put("key foo bar", "A".getBytes))
        logEntry.serialize(event)
      }
    }

    it("should throw an exception when attempting to use a key with Unicode U+000A in it when serializing.") {
      intercept[IllegalStateException] {
        val event = OrderedEvent(123L, 12345L, Put("key\nfoo\nbar", "A".getBytes))
        logEntry.serialize(event)
      }
    }
  }
}