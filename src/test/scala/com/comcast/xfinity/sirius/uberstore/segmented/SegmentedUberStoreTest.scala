package com.comcast.xfinity.sirius.uberstore.segmented

import com.comcast.xfinity.sirius.NiceTest
import java.io.File
import com.comcast.xfinity.sirius.api.impl._
import com.comcast.xfinity.sirius.api.impl.OrderedEvent
import com.comcast.xfinity.sirius.api.impl.Delete
import com.comcast.xfinity.sirius.uberstore.UberStore.{GatheringEvents, Compacting}

class SegmentedUberStoreTest extends NiceTest {

  def createTempDir(): File = {
    val tempDirName = "%s/uberstore-itest-%s".format(
      System.getProperty("java.io.tmpdir"),
      System.currentTimeMillis()
    )
    val dir = new File(tempDirName)
    dir.mkdirs()
    dir
  }

  def createFakeSegment(baseDir: File, seq: String): File = {
    val dir = new File(baseDir, seq)
    dir.mkdir

    new File(dir, "index").createNewFile()
    new File(dir, "data").createNewFile()
    dir
  }

  var uberstore: SegmentedUberStore = _
  var tempDir: File = _

  before {
    tempDir = createTempDir()
  }

  after {
    tempDir.delete
    // XXX since the tempDir is based on system time, really fast machines/tests can go spurious on us
    Thread.sleep(5)
  }

  describe("upon initialization") {
    it("should set liveDir and readOnlyDirs properly for empty uberstores") {
      uberstore = SegmentedUberStore(tempDir.getAbsolutePath)
      assert(1 === uberstore.liveDir.getNextSeq)
      assert("1" === uberstore.liveDir.name)
      assert(Nil === uberstore.readOnlyDirs)
    }
    it("should set liveDir and readOnlyDirs properly for single uberdirs") {
      createFakeSegment(tempDir, "1")
      uberstore = SegmentedUberStore(tempDir.getAbsolutePath)
      assert(1 === uberstore.liveDir.getNextSeq)
      assert("1" === uberstore.liveDir.name)
      assert(Nil === uberstore.readOnlyDirs)
    }
    it("should set liveDir and readOnlyDirs properly for multiple uberdirs") {
      createFakeSegment(tempDir, "1")
      createFakeSegment(tempDir, "5")
      createFakeSegment(tempDir, "10")
      uberstore = SegmentedUberStore(tempDir.getAbsolutePath)
      assert(1 === uberstore.liveDir.getNextSeq)
      assert("10" === uberstore.liveDir.name)
      assert(List("1", "5") === uberstore.readOnlyDirs.map(_.name))
    }
  }

  describe("writeEvent") {
    it("should write to the live uberstore only") {
      createFakeSegment(tempDir, "1")
      createFakeSegment(tempDir, "5")
      createFakeSegment(tempDir, "10")
      uberstore = SegmentedUberStore(tempDir.getAbsolutePath)
      uberstore.writeEntry(OrderedEvent(10L, 1L, Delete("1")))
      assert(1 === uberstore.readOnlyDirs(0).getNextSeq)
      // XXX uberdirs no longer base their nextSeq on the dir name, but rather the contents
      // we can stage this better once uberstore.split is implemented
      assert(1 === uberstore.readOnlyDirs(1).getNextSeq)
      assert(11 === uberstore.liveDir.getNextSeq)
    }

    it("should throw an illegalstateexception if we write out of order") {
      uberstore = SegmentedUberStore(tempDir.getAbsolutePath)
      uberstore.writeEntry(OrderedEvent(10L, 1L, Delete("1")))

      intercept[IllegalArgumentException] {
        uberstore.writeEntry(OrderedEvent(9L, 1L, Delete("1")))
      }
    }

    it("should update nextSeq based on the sequence number of the latest event") {
      uberstore = SegmentedUberStore(tempDir.getAbsolutePath)
      assert(1L === uberstore.getNextSeq)


      uberstore.writeEntry(OrderedEvent(999L, 1L, Delete("1")))
      assert(1000L === uberstore.getNextSeq)
    }
  }

  describe("getNextSeq") {
    it("should reflect livedir's nextSeq") {
      createFakeSegment(tempDir, "1")
      createFakeSegment(tempDir, "5")
      createFakeSegment(tempDir, "10")
      uberstore = SegmentedUberStore(tempDir.getAbsolutePath)
      assert(uberstore.liveDir.getNextSeq === uberstore.getNextSeq)
    }
  }

  describe("foldLeftRange") {
    it("should reflect livedir's nextSeq") {
      createFakeSegment(tempDir, "1")
      createFakeSegment(tempDir, "5")
      createFakeSegment(tempDir, "10")
      uberstore = SegmentedUberStore(tempDir.getAbsolutePath)
      uberstore.writeEntry(OrderedEvent(10L, 1L, Delete("1")))
      uberstore.writeEntry(OrderedEvent(11L, 1L, Delete("2")))
      assert(List(Delete("1"), Delete("2")) ===
        uberstore.foldLeftRange(0, Long.MaxValue)(List[SiriusRequest]())(
          (acc, event) => event.request +: acc
        ).reverse
      )
    }
  }

  describe("close") {
    it("should close all of the associated uberdirs") {
      uberstore.close()
      uberstore.readOnlyDirs.foreach(
         uberstore => assert(true === uberstore.isClosed)
      )
      assert(true === uberstore.liveDir.isClosed)
    }
  }

  describe("compact") {
    it("Should throw an UnsupportedOperationException until it's implemented") {
      uberstore = SegmentedUberStore(tempDir.getAbsolutePath)
      intercept[UnsupportedOperationException] {
        uberstore.compact()
      }
    }
  }

  describe("getCompactionState") {
    it("should return the current CompactionState") {
      uberstore = new SegmentedUberStore(createFakeSegment(tempDir, "1").getAbsolutePath) {
        state = Compacting(GatheringEvents, 100L)
      }

      assert(Compacting(GatheringEvents, 100L) === uberstore.getCompactionState)
    }
  }
}
