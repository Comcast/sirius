package com.comcast.xfinity.sirius.writeaheadlog

import com.comcast.xfinity.sirius.NiceTest
import scalax.file.Path
import org.mockito.Matchers
import org.mockito.Mockito._
import com.comcast.xfinity.sirius.api.impl.{Delete, OrderedEvent}
import com.comcast.xfinity.sirius.api.impl.persistence.BoundedLogRange
import java.io.FileNotFoundException

class CachedSiriusFileLogTest extends NiceTest {

  describe ("CachedSiriusFileLog") {
    it ("should add writes to in-mem cache") {
      val mockSerDe = mock[WALSerDe]
      val mockPath = mock[Path]
      val mockFileOps = mock[WalFileOps]

      doReturn(None).when(mockFileOps).getLastLine(Matchers.any[String])
      val underTest = new CachedSiriusFileLog("file.wal", mockSerDe, mockFileOps) {
        override lazy val file = mockPath
      }

      assert(underTest.writeCache.isEmpty)

      val event = OrderedEvent(1, 1, Delete("blah"))
      doReturn("This is what I want").when(mockSerDe).serialize(event)

      underTest.writeEntry(event)

      assert(underTest.writeCache.containsKey(1L))
    }

    it ("should trim in-mem cache if entries exceeds MAX_CACHE_SIZE") {
      val mockSerDe = mock[WALSerDe]
      val mockPath = mock[Path]
      val mockFileOps = mock[WalFileOps]

      doReturn(None).when(mockFileOps).getLastLine(Matchers.any[String])
      val underTest = new CachedSiriusFileLog("file.wal", mockSerDe, mockFileOps) {
        override lazy val MAX_CACHE_SIZE = 2
        override lazy val file = mockPath
      }

      assert(underTest.writeCache.isEmpty)

      val event1 = OrderedEvent(1, 1, Delete("blah"))
      val event2 = OrderedEvent(2, 1, Delete("blah"))
      val event3 = OrderedEvent(3, 1, Delete("blah"))
      doReturn("This is what I want").when(mockSerDe).serialize(Matchers.any[OrderedEvent])

      underTest.writeEntry(event1)
      underTest.writeEntry(event2)

      assert(underTest.writeCache.size == 2)
      assert(underTest.writeCache.containsKey(1L))
      assert(underTest.writeCache.containsKey(2L))

      underTest.writeEntry(event3)

      assert(underTest.writeCache.size == 2)
      assert(!underTest.writeCache.containsKey(1L))
      assert(underTest.writeCache.containsKey(2L))
      assert(underTest.writeCache.containsKey(3L))
    }

    it ("should return a range from the write cache if possible") {
      val mockSerDe = mock[WALSerDe]
      val mockPath = mock[Path]
      val mockFileOps = mock[WalFileOps]

      doReturn(None).when(mockFileOps).getLastLine(Matchers.any[String])
      val underTest = new CachedSiriusFileLog("file.wal", mockSerDe, mockFileOps) {
        override lazy val file = mockPath
      }

      // events in the write cache
      val events = Seq[OrderedEvent](
        OrderedEvent(1, 1, Delete("blah")), OrderedEvent(2, 1, Delete("blah")),
        OrderedEvent(3, 1, Delete("blah")), OrderedEvent(4, 1, Delete("blah"))
      )
      doReturn("This is what I want").when(mockSerDe).serialize(Matchers.any[OrderedEvent])
      events.foreach(underTest.writeEntry(_))

      val lineIterator = underTest.createIterator(BoundedLogRange(2, 3))
      assert(lineIterator.next().sequence == 2L)
      assert(lineIterator.next().sequence == 3L)
    }

    it ("should attempt to return a range from the on-disk log if not in the write cache") {
      val mockSerDe = mock[WALSerDe]
      val mockPath = mock[Path]
      val mockFileOps = mock[WalFileOps]

      doReturn(None).when(mockFileOps).getLastLine(Matchers.any[String])
      val underTest = new CachedSiriusFileLog("file.wal", mockSerDe, mockFileOps) {
        override lazy val file = mockPath
        override lazy val MAX_CACHE_SIZE = 2
      }

      // events in the write cache; only 3 and 4 will remain (MAX_CACHE_SIZE == 2)
      val events = Seq[OrderedEvent](
        OrderedEvent(1, 1, Delete("blah")), OrderedEvent(2, 1, Delete("blah")),
        OrderedEvent(3, 1, Delete("blah")), OrderedEvent(4, 1, Delete("blah"))
      )
      events.foreach(underTest.writeEntry(_))

      intercept[FileNotFoundException] {
        // not in the write cache, will try to go to disk (file.wal DNE)
        underTest.createIterator(BoundedLogRange(2, 3))
      }
    }
  }
}
