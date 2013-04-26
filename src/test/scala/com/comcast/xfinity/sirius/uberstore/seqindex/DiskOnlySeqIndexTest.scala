package com.comcast.xfinity.sirius.uberstore.seqindex

import com.comcast.xfinity.sirius.NiceTest
import java.io.File
import org.scalatest.BeforeAndAfterAll

// here's the deal, this is insane to try to test with mockery,
//  so do the real deal
class DiskOnlySeqIndexTest extends NiceTest with BeforeAndAfterAll {

  val tempDir: File = {
    val tempDirName = "%s/diskonly-seq-index-itest-%s".format(
      System.getProperty("java.io.tmpdir"),
      System.currentTimeMillis()
    )
    val dir = new File(tempDirName)
    dir.mkdirs()
    dir
  }

  override def afterAll {
    tempDir.delete()
  }

  describe("During an interesting series of events...") {
    describe ("for an empty log") {
      val underTest = DiskOnlySeqIndex(tempDir.getAbsolutePath + "/empty.index")

      it ("must return None for maxSeq when empty") {
        assert(None === underTest.getMaxSeq)
      }

      it ("must return None when some sequence number is searched for when empty") {
        assert(None === underTest.getOffsetFor(1234))
      }

      it ("must return (0, -1) when a range is searched for when empty") {
        assert((0, -1) === underTest.getOffsetRange(Long.MinValue, Long.MaxValue))
      }

      it ("must properly close") {
        assert(!underTest.isClosed)
        underTest.close()
        assert(underTest.isClosed)
      }
    }

    describe ("for a populous log") {
      val underTest = DiskOnlySeqIndex(tempDir.getAbsolutePath + "/full.index")

      it ("must be ok taking updates in the first place") {
        underTest.put(1, 0)
        underTest.put(3, 1)
        underTest.put(5, 2)
      }

      it ("must return maxSeq when known") {
        assert(Some(5) === underTest.getMaxSeq)
      }

      describe ("for getOffsetFor") {
        it ("must properly return offsets for existing offsets") {
          assert(Some(0) === underTest.getOffsetFor(1))
          assert(Some(1) === underTest.getOffsetFor(3))
          assert(Some(2) === underTest.getOffsetFor(5))
        }

        it ("must return None for nonexistent offsets") {
          assert(None === underTest.getOffsetFor(2))
          assert(None === underTest.getOffsetFor(4))
        }
      }

      describe ("for getOffsetRange") {
        it ("must return appropriate ranges when they match inclusive") {
          assert((0, 2) === underTest.getOffsetRange(1, 5))
          assert((1, 2) === underTest.getOffsetRange(3, 5))
          assert((2, 2) === underTest.getOffsetRange(5, 5))
        }

        it ("must return appropriate ranges when they do not match inclusively") {
          assert((0, 2) === underTest.getOffsetRange(Long.MinValue, Long.MaxValue))
          assert((1, 1) === underTest.getOffsetRange(2, 4))
          assert((1, 2) === underTest.getOffsetRange(2, Long.MaxValue))
          assert((0, 1) === underTest.getOffsetRange(Long.MinValue, 4))
        }

        it ("must return (0, -1) for bogus ranges") {
          assert((0, -1) === underTest.getOffsetRange(Long.MaxValue, Long.MinValue))
        }

        it ("must return (0, -1) for empty ranges") {
          assert((0, -1) === underTest.getOffsetRange(Long.MinValue, 0))
          assert((0, -1) === underTest.getOffsetRange(6, Long.MaxValue))
          assert((0, -1) === underTest.getOffsetRange(2, 2))
          assert((0, -1) === underTest.getOffsetRange(4, 4))
          assert((0, -1) === underTest.getOffsetRange(0, 0))
        }

        it ("must properly close") {
          assert(!underTest.isClosed)
          underTest.close()
          assert(underTest.isClosed)
        }
      }
    }
  }
}