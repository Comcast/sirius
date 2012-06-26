package com.comcast.xfinity.sirius.writeaheadlog

import org.mockito.Mockito._
import scalax.io.Line.Terminators.NewLine
import java.io.ByteArrayInputStream
import com.comcast.xfinity.sirius.NiceTest
import scalax.file.defaultfs.DefaultPath
import org.mockito.Matchers
import scalax.io.{Codec, Resource}

class FileLogReaderTest extends NiceTest {

  var reader: SiriusFileLog = _
  var mockPath: DefaultPath = _
  var mockSerDe: LogDataSerDe = _

  val FILENAME = "fake_file"
  val LOG_DATA1 = LogData("PUT", "key1 foo bar", 123L, 12345L, Some(Array[Byte](65)))
  val FIRST_RAW_LINE = "firstLine\n"
  val SECOND_RAW_LINE = "secondLine\n"
  val LOG_DATA2 = LogData("DELETE", "key2 foo bar", 123L, 12345L, None)


  before {
    mockSerDe = mock[LogDataSerDe]
    mockPath = mock[DefaultPath]
    reader = new SiriusFileLog(FILENAME, mockSerDe) {
      override val file = mockPath
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

      val result = reader.foldLeft[List[LogData]](Nil)((acc, logData) => logData :: acc)

      assert(result.reverse === List(LOG_DATA1, LOG_DATA2))
    }
  }

}