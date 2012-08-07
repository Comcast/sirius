package com.comcast.xfinity.sirius.writeaheadlog

import org.mockito.Mockito._
import com.comcast.xfinity.sirius.NiceTest
import java.io.ByteArrayInputStream
import scalax.io.Line.Terminators.NewLine
import org.mockito.Matchers
import scalax.io.{LongTraversable, Line, Codec, Resource}
import com.comcast.xfinity.sirius.api.impl.{Delete, OrderedEvent, Put}
import scalax.file.Path

class SiriusFileLogTest extends NiceTest {

  var mockSerDe: WALSerDe = _
  var mockPath: Path = _
  var mockFileOps: WalFileOps = _
  var log: SiriusFileLog = _

  describe("SiriusFileLog") {
    it ("must properly acquire a nextPossibleSeq from the last line of a log that exists") {
      mockSerDe = mock[WALSerDe]
      mockFileOps = mock[WalFileOps]

      val fileName = "file.wal"
      val returnedEvent: OrderedEvent = OrderedEvent(3L, 0L, Delete("asdf"))

      when(mockFileOps.getLastLine(fileName)).thenReturn(Some(""))
      when(mockSerDe.deserialize(Matchers.any[String])).thenReturn(returnedEvent)

      log = new SiriusFileLog(fileName, mockSerDe, mockFileOps) {
        override lazy val file = mockPath
      }

      assert(4L === log.getNextSeq)
    }

    it ("must set a nextPossibleSeq = 1 if None is returned from WalFileOps.getLastLine") {
      mockFileOps = mock[WalFileOps]

      val fileName = "file.wal"
      when(mockFileOps.getLastLine(fileName)).thenReturn(None)

      log = new SiriusFileLog(fileName, mockSerDe, mockFileOps) {
        override lazy val file = mockPath
      }

      assert(1L === log.getNextSeq)
    }


    describe(".writeEntry") {
      it ("must append the event as serialized by the SerDe to the file") {
        val mockSerDe = mock[WALSerDe]
        val mockPath = mock[Path]
        val mockFileOps = mock[WalFileOps]

        doReturn(None).when(mockFileOps).getLastLine(Matchers.any[String])
        val log = new SiriusFileLog("file.wal", mockSerDe, mockFileOps) {
          override lazy val file = mockPath
        }

        val event = OrderedEvent(1, 1, Delete("blah"))
        doReturn("This is what I want").when(mockSerDe).serialize(event)

        log.writeEntry(event)

        verify(mockPath).append(Matchers.eq("This is what I want"))
      }

      it ("must throw an exception if events are written out of order") {
        val mockSerDe = mock[WALSerDe]
        val mockPath = mock[Path]
        val mockFileOps = mock[WalFileOps]

        doReturn(None).when(mockFileOps).getLastLine(Matchers.any[String])
        val log = new SiriusFileLog("file.wal", mockSerDe, mockFileOps) {
          override lazy val file = mockPath
        }

        log.writeEntry(OrderedEvent(2, 1, Delete("blah")))
        intercept[IllegalArgumentException] {
          log.writeEntry(OrderedEvent(1, 1, Delete("blah")))
        }
      }
    }

    describe(".foldLeft") {
      it("deserialize and fold left over the entries in the file in order") {
        val EVENT1 = OrderedEvent(123L, 12345L, Put("key1 foo bar", "A".getBytes))
        val EVENT2 = OrderedEvent(123L, 12345L, Delete("key2 foo bar"))
        val FIRST_RAW_LINE = "firstLine\n"
        val SECOND_RAW_LINE = "secondLine\n"

        mockSerDe = mock[WALSerDe]
        mockPath = mock[Path]
        mockFileOps = mock[WalFileOps]

        doReturn(None).when(mockFileOps).getLastLine(Matchers.any[String])
        log = new SiriusFileLog("file.wal", mockSerDe, mockFileOps) {
          override lazy val file = mockPath
        }

        val lineTraversable = LongTraversable(FIRST_RAW_LINE, SECOND_RAW_LINE)
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