package com.comcast.xfinity.sirius.writeaheadlog

import org.mockito.Mockito._
import scalax.file.defaultfs.DefaultPath
import com.comcast.xfinity.sirius.NiceTest

class FileLogWriterTest extends NiceTest {

  var writer: SiriusFileLog = _
  var mockSerDe: LogDataSerDe = _
  var mockPath: DefaultPath = _

  val SERIALIZED: String = "some data"
  val FILENAME: String = "fake_file"


  before {
    mockSerDe = mock[LogDataSerDe]
    mockPath = mock[DefaultPath]
    writer = spy(new SiriusFileLog(FILENAME, mockSerDe))

  }

  describe("SiriusFileLog") {
    describe(".writeEntry") {
      it("appends a serialized entry to a file") {
        val logData = new LogData("PUT", "key", 123L, 12345L, Some(Array[Byte](65, 124, 65)))
        when(mockSerDe.serialize(logData)).thenReturn(SERIALIZED)
        doReturn(mockPath).when(writer).file

        writer.writeEntry(logData)

        verify(mockPath).append(SERIALIZED)

      }
    }
  }

}