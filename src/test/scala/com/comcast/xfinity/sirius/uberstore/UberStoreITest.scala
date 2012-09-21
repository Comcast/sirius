package com.comcast.xfinity.sirius.uberstore

import java.io.File
import org.scalatest.BeforeAndAfterAll
import com.comcast.xfinity.sirius.NiceTest
import com.comcast.xfinity.sirius.api.impl.{Delete, OrderedEvent}
import com.comcast.xfinity.sirius.api.impl.persistence.{BoundedLogRange, EntireLog}

class UberStoreITest extends NiceTest with BeforeAndAfterAll {

  val tempDir: File = {
    val tempDirName = "%s/uberstore-itest-%s".format(
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

  // XXX: not these sub tasks are not parallelizable
  describe("During an interesting series of events...") {
    var uberStore = UberStore(tempDir.getAbsolutePath)

    val getAllEvents: UberStore => List[OrderedEvent] =
      _.foldLeft(List[OrderedEvent]())((a, e) => e :: a).reverse

    it ("must have no events when empty") {
      assert(Nil === getAllEvents(uberStore))
    }

    val event1 = OrderedEvent(2, 3, Delete("yo mamma"))
    it ("must have one event in a single entry log") {
      uberStore.writeEntry(event1)
      assert(List(event1) === getAllEvents(uberStore))
    }

    it ("must not write events out of order") {
      intercept[IllegalArgumentException] {
        uberStore.writeEntry(OrderedEvent(1, 5, Delete("is so fat")))
      }
    }

    val event2 = OrderedEvent(3, 5, Delete("the earth circles her"))
    it ("must also accept some more entries") {
      uberStore.writeEntry(event2)
      assert(List(event1, event2) === getAllEvents(uberStore))
    }

    it ("must properly report the next seq properly") {
      assert(4L === uberStore.getNextSeq)
    }

    val event3 = OrderedEvent(5, 678, Delete("that she's fat"))
    it ("must also be transferable to another handle") {
      // XXX: One UberStore to rule them all, hide the other one so
      //      we dont' accidentally use it
      uberStore = UberStore(tempDir.getAbsolutePath)

      assert(4L === uberStore.getNextSeq)
      assert(List(event1, event2) === getAllEvents(uberStore))

      uberStore.writeEntry(event3)
      assert(List(event1, event2, event3) === getAllEvents(uberStore))
    }

    it ("must be able to recover from a missing index") {
      val file = new File(tempDir, "1.index")
      assert(file.exists(), "Your test is hosed, expecting 1.index to exist")
      file.delete()
      assert(!file.exists(), "Your test is hosed, expecting 1.index to be bye bye")

      uberStore = UberStore(tempDir.getAbsolutePath)
      assert(6L === uberStore.getNextSeq)
      assert(List(event1, event2, event3) === getAllEvents(uberStore))
    }
  }
}