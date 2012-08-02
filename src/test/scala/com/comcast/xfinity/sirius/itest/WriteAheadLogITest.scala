package com.comcast.xfinity.sirius.itest

import org.junit.rules.TemporaryFolder
import com.comcast.xfinity.sirius.writeaheadlog._

import com.comcast.xfinity.sirius.api.impl._

import com.comcast.xfinity.sirius.{TimedTest, NiceTest}

object WriteAheadLogITest {
  def readEntries(siriusLog: SiriusLog) =
    siriusLog.foldLeft(List[OrderedEvent]())(
      (acc, event) => event :: acc
    ).reverse
}

class WriteAheadLogITest extends NiceTest {

  import WriteAheadLogITest._

  val tempFolder = new TemporaryFolder()

  var siriusLog: SiriusFileLog = _

  before {
    tempFolder.create()

    val logFilename = tempFolder.newFile("sirius_wal.log").getAbsolutePath
    siriusLog = SiriusFileLog(logFilename)
  }

  after {
    tempFolder.delete()
  }

  describe("a Sirius Write Ahead Log") {
    it("should have 1 entry after a PUT") {
      val entries = List(
        OrderedEvent(1, 1234, Put("1", "some body".getBytes))
      )

      entries.foreach(siriusLog.writeEntry(_))

      assert(entries === readEntries(siriusLog))
    }

    it("should have 2 entries after 2 PUTs") {
      val entries = List(
        OrderedEvent(1, 1234, Put("1", "some body".getBytes)),
        OrderedEvent(2, 1235, Put("2", "some other body".getBytes))
      )

      entries.foreach(siriusLog.writeEntry(_))

      assert(entries === readEntries(siriusLog))
    }

    it("should have a PUT and a DELETE entry after a PUT and a DELETE") {
      val entries = List(
        OrderedEvent(1, 1234, Put("1", "some body".getBytes)),
        OrderedEvent(2, 1236, Delete("1"))
      )

      entries.foreach(siriusLog.writeEntry(_))

      assert(entries === readEntries(siriusLog))
    }

  }
}