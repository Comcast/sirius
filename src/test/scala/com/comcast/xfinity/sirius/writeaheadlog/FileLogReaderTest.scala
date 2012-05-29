package com.comcast.xfinity.sirius.writeaheadlog

import org.mockito.Mockito._
import scalax.io.Line.Terminators.NewLine
import scalax.io.Resource
import java.io.ByteArrayInputStream
import com.comcast.xfinity.sirius.NiceTest
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

class FileLogReaderTest extends NiceTest{

  var reader: FileLogReader = _
  var mockSerDe: LogDataSerDe = _

  val FILENAME = "fake_file"
  val LOG_DATA1 = LogData("PUT", "key1 foo bar", 123L, 12345L, Some(Array[Byte](65)))
  val FIRST_RAW_LINE = "firstLine\n"
  val SECOND_RAW_LINE = "secondLine\n"
  val LOG_DATA2 = LogData("DELETE", "key2 foo bar", 123L, 12345L, None)


  before {
    mockSerDe = mock[LogDataSerDe]
    reader = spy(new FileLogReader(FILENAME, mockSerDe))
  }

  describe("FileLogReader") {
    describe(".readEntries") {
      it("should read, deserialize and then handle each entry in a file") {
        when(mockSerDe.deserialize(FIRST_RAW_LINE)).thenReturn(LOG_DATA1)
        when(mockSerDe.deserialize(SECOND_RAW_LINE)).thenReturn(LOG_DATA2)

        val rawLines = FIRST_RAW_LINE + SECOND_RAW_LINE
        val lineTraversable = Resource.fromInputStream(new ByteArrayInputStream(rawLines.getBytes)).lines(NewLine, true)
        doReturn(lineTraversable).when(reader).lines

        var actualLogData: List[LogData] = List[LogData]()

        val fn: LogData => Unit = (logData: LogData) => {
          actualLogData = actualLogData ::: List(logData)

        }

        reader.readEntries(fn)

        assert(LOG_DATA1 === actualLogData(0))
        assert(LOG_DATA2 === actualLogData(1))

      }
    }
  }

}