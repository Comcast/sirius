package com.comcast.xfinity.sirius.writeaheadlog

import org.mockito.Mockito._
import scalax.file.defaultfs.DefaultPath
import com.comcast.xfinity.sirius.NiceTest
import java.io.ByteArrayInputStream
import scalax.io.Line.Terminators.NewLine
import org.mockito.Matchers
import scalax.io.{Codec, Resource}
import com.comcast.xfinity.sirius.api.impl.{Delete, OrderedEvent, Put}

class SiriusFileLogTest extends NiceTest {

  var mockSerDe: WALSerDe = _
  var mockPath: DefaultPath = _
  var log: SiriusFileLog = _


  val SERIALIZED: String = "some data"
  val FILENAME: String = "fake_file"
  val EVENT1 = OrderedEvent(123L, 12345L, Put("key1 foo bar", "A".getBytes))
  val EVENT2 = OrderedEvent(123L, 12345L, Delete("key2 foo bar"))
  val FIRST_RAW_LINE = "firstLine\n"
  val SECOND_RAW_LINE = "secondLine\n"

  before {
    mockSerDe = mock[WALSerDe]
    mockPath = mock[DefaultPath]
    log = new SiriusFileLog(FILENAME, mockSerDe) {
      override val file = mockPath
    }
  }

  describe("SiriusFileLog") {
    describe(".writeEntry") {
      it("appends a serialized entry to a file") {
        val event = OrderedEvent(123L, 12345L, Put("key", "A".getBytes))
        when(mockSerDe.serialize(event)).thenReturn(SERIALIZED)

        log.writeEntry(event)

        verify(mockPath).append(SERIALIZED)
      }
    }

    describe(".foldLeft") {
      it("deserialize and fold left over the entries in the file in order") {
        val rawLines = FIRST_RAW_LINE + SECOND_RAW_LINE
        val lineTraversable = Resource.fromInputStream(new ByteArrayInputStream(rawLines.getBytes)).lines(NewLine, true)
        doReturn(lineTraversable).when(mockPath).lines(
          Matchers.eq(NewLine), Matchers.anyBoolean())(Matchers.any(classOf[Codec]))

        when(mockSerDe.deserialize(FIRST_RAW_LINE)).thenReturn(EVENT1)
        when(mockSerDe.deserialize(SECOND_RAW_LINE)).thenReturn(EVENT2)

        val result = log.foldLeft[List[OrderedEvent]](Nil)((acc, logData) => logData :: acc)

        assert(result.reverse === List(EVENT1, EVENT2))
      }
    }
  }
}