package com.comcast.xfinity.sirius.writeaheadlog

import scalax.file.defaultfs.DefaultPath
import com.comcast.xfinity.sirius.NiceTest
import org.mockito.Mockito._
import org.mockito.Matchers
import scalax.io.{Codec, Resource}
import scalax.io.Line.Terminators.NewLine
import java.io.ByteArrayInputStream
import com.comcast.xfinity.sirius.api.impl.{Delete, OrderedEvent, Put}

class SiriusFileLogCompactorTest extends NiceTest {
  var logCompactor: SiriusFileLogCompactor = _
  var mockSerDe: WALSerDe = _
  var mockPath: DefaultPath = _
  var log: SiriusFileLog = _

  val FILENAME: String = "fake_file"
  val LOG_RAW_1 = "firstLine1\n"
  val LOG_RAW_2 = "firstLine2\n"
  val LOG_RAW_3 = "firstLine3\n"
  val LOG_RAW_4 = "firstLine4\n"
  val LOG_RAW_5 = "firstLine5\n"
  val LOG_RAW_6 = "firstLine6\n"
  val LOG_RAW_7 = "firstLine7\n"
  val LOG_RAW_8 = "firstLine8\n"
  val LOG_RAW_9 = "firstLine9\n"
  val LOG_RAW_10 = "firstLine10\n"

  val EVENT1 = OrderedEvent(1, 12345L, Put("key1", "A".getBytes))
  val EVENT2 = OrderedEvent(2, 12345L, Delete("key2"))
  val EVENT3 = OrderedEvent(3, 12345L, Put("key3", "A".getBytes))
  val EVENT4 = OrderedEvent(4, 12345L, Delete("key2"))
  val EVENT5 = OrderedEvent(5, 12345L, Put("key1", "A".getBytes))
  val EVENT6 = OrderedEvent(6, 12345L, Put("key2", "A".getBytes))
  val EVENT7 = OrderedEvent(7, 12345L, Put("key1", "A".getBytes))
  val EVENT8 = OrderedEvent(8, 12345L, Put("key2", "A".getBytes))
  val EVENT9 = OrderedEvent(9, 12345L, Put("key1", "A".getBytes))
  val EVENT10 = OrderedEvent(10, 12345L, Delete("key2"))

  before {
    mockSerDe = mock[WALSerDe]
    mockPath = mock[DefaultPath]
    log = new SiriusFileLog(FILENAME, mockSerDe) {
      override val file = mockPath
    }
    logCompactor = new SiriusFileLogCompactor(log);
  }

  describe(".compactLog") {
      it("compact the log entries in SiriusFileLog") {
        val rawLines = LOG_RAW_1 + LOG_RAW_2 + LOG_RAW_3 + LOG_RAW_4 + LOG_RAW_5 +
                       LOG_RAW_6 + LOG_RAW_7 + LOG_RAW_8 + LOG_RAW_9 + LOG_RAW_10
        val lineTraversable = Resource.fromInputStream(new ByteArrayInputStream(
            rawLines.getBytes)).lines(NewLine, true)
        doReturn(lineTraversable).when(mockPath).lines(
          Matchers.eq(NewLine), Matchers.anyBoolean())(Matchers.any(classOf[Codec]))

        when(mockSerDe.deserialize(LOG_RAW_1)).thenReturn(EVENT1)
        when(mockSerDe.deserialize(LOG_RAW_2)).thenReturn(EVENT2)
        when(mockSerDe.deserialize(LOG_RAW_3)).thenReturn(EVENT3)
        when(mockSerDe.deserialize(LOG_RAW_4)).thenReturn(EVENT4)
        when(mockSerDe.deserialize(LOG_RAW_5)).thenReturn(EVENT5)
        when(mockSerDe.deserialize(LOG_RAW_6)).thenReturn(EVENT6)
        when(mockSerDe.deserialize(LOG_RAW_7)).thenReturn(EVENT7)
        when(mockSerDe.deserialize(LOG_RAW_8)).thenReturn(EVENT8)
        when(mockSerDe.deserialize(LOG_RAW_9)).thenReturn(EVENT9)
        when(mockSerDe.deserialize(LOG_RAW_10)).thenReturn(EVENT10)

        val result = logCompactor.compactLog();

        assert(result === List(EVENT3, EVENT9, EVENT10))
      }
    }
}