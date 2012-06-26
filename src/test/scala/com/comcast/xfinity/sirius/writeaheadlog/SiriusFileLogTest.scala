package com.comcast.xfinity.sirius.writeaheadlog

import org.mockito.Mockito._
import scalax.file.defaultfs.DefaultPath
import com.comcast.xfinity.sirius.NiceTest
import java.io.ByteArrayInputStream
import scalax.io.Line.Terminators.NewLine
import org.mockito.Matchers
import scalax.io.{Codec, Resource}

class SiriusFileLogTest extends NiceTest {

  var mockSerDe: LogDataSerDe = _
  var mockPath: DefaultPath = _
  var log: SiriusFileLog = _


  val SERIALIZED: String = "some data"
  val FILENAME: String = "fake_file"
  val LOG_DATA1 = LogData("PUT", "key1 foo bar", 123L, 12345L, Some(Array[Byte](65)))
  val FIRST_RAW_LINE = "firstLine\n"
  val SECOND_RAW_LINE = "secondLine\n"
  val LOG_DATA2 = LogData("DELETE", "key2 foo bar", 123L, 12345L, None)

  before {
    mockSerDe = mock[LogDataSerDe]
    mockPath = mock[DefaultPath]
    log = new SiriusFileLog(FILENAME, mockSerDe) {
      override val file = mockPath
    }
  }

  describe("SiriusFileLog") {
    describe(".writeEntry") {
      it("appends a serialized entry to a file") {
        val logData = new LogData("PUT", "key", 123L, 12345L, Some(Array[Byte](65, 124, 65)))
        when(mockSerDe.serialize(logData)).thenReturn(SERIALIZED)

        log.writeEntry(logData)

        verify(mockPath).append(SERIALIZED)
      }
    }

    describe(".foldLeft") {
      it("deserialize and fold left over the entries in the file in order") {
        val rawLines = FIRST_RAW_LINE + SECOND_RAW_LINE
        val lineTraversable = Resource.fromInputStream(new ByteArrayInputStream(rawLines.getBytes)).lines(NewLine, true)
        doReturn(lineTraversable).when(mockPath).lines(
          Matchers.eq(NewLine), Matchers.anyBoolean())(Matchers.any(classOf[Codec]))

        when(mockSerDe.deserialize(FIRST_RAW_LINE)).thenReturn(LOG_DATA1)
        when(mockSerDe.deserialize(SECOND_RAW_LINE)).thenReturn(LOG_DATA2)

        val result = log.foldLeft[List[LogData]](Nil)((acc, logData) => logData :: acc)

        assert(result.reverse === List(LOG_DATA1, LOG_DATA2))
      }
    }
  }
}