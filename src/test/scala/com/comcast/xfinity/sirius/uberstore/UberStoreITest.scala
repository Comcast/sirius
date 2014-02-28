/*
 *  Copyright 2012-2014 Comcast Cable Communications Management, LLC
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.comcast.xfinity.sirius.uberstore

import java.io.File
import org.scalatest.BeforeAndAfterAll
import com.comcast.xfinity.sirius.NiceTest
import com.comcast.xfinity.sirius.api.impl.{Delete, OrderedEvent}
import scalax.file.Path

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
    Path(tempDir).deleteRecursively(force = true)
  }

  // XXX: not these sub tasks are not parallelizable
  describe("During an interesting series of events...") {
    var uberStore = UberStore(tempDir.getAbsolutePath)

    it ("must have no events when empty") {
      assert(Nil === getAllEvents(uberStore))
    }

    it ("must properly report the next sequence number when empty") {
      assert(1L === uberStore.getNextSeq)
    }

    val events1Through100 = generateEvents(1, 100)
    it ("must be able to accept and retain a bunch of Delete events") {
      events1Through100.foreach(uberStore.writeEntry)
      assert(events1Through100 === getAllEvents(uberStore))
    }

    it ("must not accept an event out of order") {
      intercept[IllegalArgumentException] {
        uberStore.writeEntry(OrderedEvent(1, 5, Delete("is so fat")))
      }
    }

    it ("must properly report the next sequence number when dataful") {
      assert(101L === uberStore.getNextSeq)
    }

    val events101Through200 = generateEvents(101, 200)
    it ("must cleanly transfer to a new handle") {
      // XXX: One UberStore to rule them all, close and hide the other one so
      //      we dont' accidentally use it
      uberStore.close()
      uberStore = UberStore(tempDir.getAbsolutePath)

      assert(101L === uberStore.getNextSeq)
      assert(events1Through100 === getAllEvents(uberStore))

      events101Through200.foreach(uberStore.writeEntry)
      assert(201L === uberStore.getNextSeq)
      assert(events1Through100 ++ events101Through200 === getAllEvents(uberStore))
    }

    it ("must be able to recover from a missing index") {
      val file = new File(tempDir, "1.index")
      assert(file.exists(), "Your test is hosed, expecting 1.index to exist")
      file.delete()
      assert(!file.exists(), "Your test is hosed, expecting 1.index to be bye bye")

      assert(events1Through100 ++ events101Through200 === getAllEvents(uberStore))
    }

    it ("must report size correctly"){
      uberStore.writeEntry(OrderedEvent(uberStore.getNextSeq, 5, Delete("is so fat")))
      assert(8541L === uberStore.size)
    }
  }

  private def generateEvents(start: Long, end: Long): List[OrderedEvent] =
    List.range(start, end + 1).map(n => OrderedEvent(n, n + 1000L, Delete(n.toString)))

  private def getAllEvents(uberStore: UberStore): List[OrderedEvent] = {
    val reversedEvents = uberStore.foldLeft(List[OrderedEvent]())((acc, e) => e :: acc)
    reversedEvents.reverse
  }
}
