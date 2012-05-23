package com.comcast.xfinity.sirius.writeaheadlog

import org.scalatest.{FunSpec, BeforeAndAfter}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito._
import scalax.file.defaultfs.DefaultPath

@RunWith(classOf[JUnitRunner])
class FileLogWriterTest extends FunSpec with BeforeAndAfter with MockitoSugar {

  var writer: FileLogWriter = _
  var mockSerDe: LogDataSerDe = _
  var mockPath: DefaultPath = _

  val SERIALIZED: String = "some data"
  val FILENAME: String = "fake_file"


  before {
    mockSerDe = mock[LogDataSerDe]
    mockPath = mock[DefaultPath]
    writer = spy(new FileLogWriter(FILENAME, mockSerDe))

  }

  describe("FileLogWriter") {
    describe(".writeEntry") {
      it("appends a serialized entry to a file") {
        val logData = new LogData("PUT", "key", 123L, 12345L, Array[Byte](65, 124, 65))
        when(mockSerDe.serialize(logData)).thenReturn(SERIALIZED)
        doReturn(mockPath).when(writer).file

        writer.writeEntry(logData)

        verify(mockPath).append(SERIALIZED)

      }
    }
  }

}