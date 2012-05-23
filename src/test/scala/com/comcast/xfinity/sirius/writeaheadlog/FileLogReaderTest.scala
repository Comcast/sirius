package com.comcast.xfinity.sirius.writeaheadlog

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfter, FunSpec}
import org.mockito.Mockito._
import scalax.io.Line.Terminators.CarriageReturn
import scalax.io.{Resource, LineTraversable, LongTraversable}
import java.io.ByteArrayInputStream

@RunWith(classOf[JUnitRunner])
class FileLogReaderTest extends FunSpec with BeforeAndAfter with MockitoSugar {

  var reader: FileLogReader = _
  var mockSerDe: LogDataSerDe = _

  val FILENAME = "fake_file"
  val LOG_DATA1 = LogData("PUT", "key1 foo bar", 123L, 12345L, Array[Byte](65))
  val FIRST_RAW_LINE = "firstLine"
  val SECOND_RAW_LINE = "secondLine"
  val LOG_DATA2 = LogData("PUT", "key2 foo bar", 123L, 12345L, Array[Byte](65))


  before {
    mockSerDe = mock[LogDataSerDe]
    reader = spy(new FileLogReader(FILENAME, mockSerDe))
  }

  describe("FileLogReader") {
    describe(".readEntries") {
      it("should read, deserialize and then handle each entry in a file") {
        when(mockSerDe.deserialize(FIRST_RAW_LINE)).thenReturn(LOG_DATA1)
        when(mockSerDe.deserialize(SECOND_RAW_LINE)).thenReturn(LOG_DATA2)

        val rawLines = FIRST_RAW_LINE + CarriageReturn.sep + SECOND_RAW_LINE + CarriageReturn.sep
        val lineTraversable = Resource.fromInputStream(new ByteArrayInputStream(rawLines.getBytes)).lines(CarriageReturn)
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