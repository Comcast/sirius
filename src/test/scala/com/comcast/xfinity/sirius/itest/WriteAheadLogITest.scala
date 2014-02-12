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

package com.comcast.xfinity.sirius.itest

import org.junit.rules.TemporaryFolder
import com.comcast.xfinity.sirius.writeaheadlog._

import com.comcast.xfinity.sirius.api.impl._

import com.comcast.xfinity.sirius.{TimedTest, NiceTest}
import java.io.File
import com.comcast.xfinity.sirius.uberstore.UberStore

object WriteAheadLogITest {
  def readEntries(siriusLog: SiriusLog) =
    siriusLog.foldLeft(List[OrderedEvent]())(
      (acc, event) => event :: acc
    ).reverse
}

class WriteAheadLogITest extends NiceTest {

  import WriteAheadLogITest._

  val tempFolder = new TemporaryFolder()

  var siriusLog: SiriusLog = _

  before {
    tempFolder.create()

    val uberstorePath = tempFolder.newFolder("uberstore")
    siriusLog = UberStore(uberstorePath.getAbsolutePath)
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
