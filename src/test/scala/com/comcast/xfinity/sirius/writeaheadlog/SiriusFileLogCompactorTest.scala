package com.comcast.xfinity.sirius.writeaheadlog

import scalax.file.defaultfs.DefaultPath
import com.comcast.xfinity.sirius.NiceTest
import org.mockito.Mockito._
import org.mockito.Matchers
import scalax.io.{Codec, Resource}
import scalax.io.Line.Terminators.NewLine
import java.io.ByteArrayInputStream

class SiriusFileLogCompactorTest extends NiceTest {
  var logCompactor: SiriusFileLogCompactor = _
  var mockSerDe: LogDataSerDe = _
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
    
  val LOG_DATA_1 = LogData("PUT", "key1 foo bar", 123L, 12345L, Some(Array[Byte](65)))
  val LOG_DATA_2 = LogData("DELETE", "key2 foo bar", 124L, 12345L, None)
  val LOG_DATA_3 = LogData("PUT", "key3 foo bar", 125L, 12345L, Some(Array[Byte](65)))
  val LOG_DATA_4 = LogData("DELETE", "key2 foo bar", 126L, 12345L, None)
  val LOG_DATA_5 = LogData("PUT", "key1 foo bar", 127L, 12345L, Some(Array[Byte](65)))
  val LOG_DATA_6 = LogData("PUT", "key2 foo bar", 128L, 12345L, None)
  val LOG_DATA_7 = LogData("PUT", "key1 foo bar", 129L, 12345L, Some(Array[Byte](65)))
  val LOG_DATA_8 = LogData("PUT", "key2 foo bar", 130L, 12345L, None)
  val LOG_DATA_9 = LogData("PUT", "key1 foo bar", 131L, 12345L, Some(Array[Byte](65)))
  val LOG_DATA_10 = LogData("DELETE", "key2 foo bar", 132L, 12345L, None)
  

  before {
    mockSerDe = mock[LogDataSerDe]
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

        when(mockSerDe.deserialize(LOG_RAW_1)).thenReturn(LOG_DATA_1)
        when(mockSerDe.deserialize(LOG_RAW_2)).thenReturn(LOG_DATA_2)
        when(mockSerDe.deserialize(LOG_RAW_3)).thenReturn(LOG_DATA_3)
        when(mockSerDe.deserialize(LOG_RAW_4)).thenReturn(LOG_DATA_4)
        when(mockSerDe.deserialize(LOG_RAW_5)).thenReturn(LOG_DATA_5)
        when(mockSerDe.deserialize(LOG_RAW_6)).thenReturn(LOG_DATA_6)
        when(mockSerDe.deserialize(LOG_RAW_7)).thenReturn(LOG_DATA_7)
        when(mockSerDe.deserialize(LOG_RAW_8)).thenReturn(LOG_DATA_8)
        when(mockSerDe.deserialize(LOG_RAW_9)).thenReturn(LOG_DATA_9)
        when(mockSerDe.deserialize(LOG_RAW_10)).thenReturn(LOG_DATA_10)

        val result = logCompactor.compactLog();

        assert(result === List(LOG_DATA_3, LOG_DATA_9, LOG_DATA_10))
      }
    }
}