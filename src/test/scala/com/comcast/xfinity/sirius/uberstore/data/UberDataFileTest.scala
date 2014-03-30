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
package com.comcast.xfinity.sirius.uberstore.data

import com.comcast.xfinity.sirius.NiceTest
import java.io.RandomAccessFile
import org.mockito.Mockito._
import com.comcast.xfinity.sirius.api.impl.{OrderedEvent, Delete}
import org.mockito.Matchers.{any, same}

class UberDataFileTest extends NiceTest {

  it ("must seek to the end of the write file handle on instantiation") {
    val mockWriteHandle = mock[RandomAccessFile]
    val mockDesc = mock[UberDataFile.UberFileDesc]
    doReturn(mockWriteHandle).when(mockDesc).createWriteHandle()

    val mockFileOps = mock[UberStoreFileOps]
    val mockCodec = mock[OrderedEventCodec]

    doReturn(100L).when(mockWriteHandle).length

    new UberDataFile(mockDesc, mockFileOps, mockCodec)

    verify(mockWriteHandle).seek(100L)
  }

  describe("writeEvent") {
    it ("must serialize the event and delegate its persistence to fileOps") {
      val mockWriteHandle = mock[RandomAccessFile]
      val mockDesc = mock[UberDataFile.UberFileDesc]
      doReturn(mockWriteHandle).when(mockDesc).createWriteHandle()

      val mockFileOps = mock[UberStoreFileOps]
      val mockCodec = mock[OrderedEventCodec]

      val underTest = new UberDataFile(mockDesc, mockFileOps, mockCodec)

      val theEvent = OrderedEvent(1, 2, Delete("hello world"))
      val serialized = "i award you know points, and may god have mercy on your soul".getBytes
      doReturn(serialized).when(mockCodec).serialize(theEvent)

      doReturn(1000L).when(mockFileOps).put(any[RandomAccessFile], any[Array[Byte]])

      assert(1000L === underTest.writeEvent(theEvent))

      verify(mockFileOps).put(same(mockWriteHandle), same(serialized))
    }
  }

  describe("foldLeft") {
    it ("must fold over the entire content of the file, invoking fileOps.readNext until there are None, " +
        "and closes the read handle on completion") {
      val mockWriteHandle = mock[RandomAccessFile]
      val mockReadHandle = mock[RandomAccessFile]
      val mockDesc = mock[UberDataFile.UberFileDesc]
      doReturn(mockWriteHandle).when(mockDesc).createWriteHandle()
      doReturn(mockReadHandle).when(mockDesc).createReadHandle()
      doReturn(51L).when(mockReadHandle).length

      val mockFileOps = mock[UberStoreFileOps]
      val mockCodec = mock[OrderedEventCodec]

      val underTest = new UberDataFile(mockDesc, mockFileOps, mockCodec)

      // Need to simulate 3 successful reads from the handle, followed by None indicating we hit the end
      val dummyBytes = "dummy".getBytes
      doReturn(Some(dummyBytes, 17L))
        .doReturn(Some(dummyBytes, 34L))
        .doReturn(Some(dummyBytes, 51L))
        .doReturn(None).
        when(mockFileOps).readNext(any[RandomAccessFile], any[Long], any[Long])

      // Need to simulate the conversion of the events from above becoming OrderedEvents
      val event1 = OrderedEvent(1, 2, Delete("a"))
      val event2 = OrderedEvent(2, 3, Delete("b"))
      val event3 = OrderedEvent(3, 4, Delete("c"))
      doReturn(event1).doReturn(event2).doReturn(event3).
        when(mockCodec).deserialize(any[Array[Byte]])

      val result = underTest.foldLeft(List[(Long, OrderedEvent)]())(
        (acc, off, evt) => (off, evt) :: acc
      ).reverse

      assert(List((0L, event1), (17L, event2), (34L, event3)) === result)

      // verify that we read and deserialized the expected number of times
      verify(mockFileOps, times(4)).readNext(same(mockReadHandle), any[Long], any[Long])
      verify(mockFileOps).readNext(mockReadHandle, 0, 51L)
      verify(mockFileOps).readNext(mockReadHandle, 17L, 51L)
      verify(mockFileOps).readNext(mockReadHandle, 34L, 51L)
      verify(mockFileOps).readNext(mockReadHandle, 51L, 51L)
      verify(mockCodec, times(3)).deserialize(same(dummyBytes))

      // also verify cleanup!
      verify(mockReadHandle).close()
    }
  }

  describe("foldLeftRange") {
    it ("must only iterate over the specified range of offsets, inclusive, and when finished close the handle") {
      val mockWriteHandle = mock[RandomAccessFile]
      val mockReadHandle = mock[RandomAccessFile]
      val mockDesc = mock[UberDataFile.UberFileDesc]
      doReturn(mockWriteHandle).when(mockDesc).createWriteHandle()
      doReturn(mockReadHandle).when(mockDesc).createReadHandle()
      doReturn(168L).when(mockReadHandle).length

      val mockFileOps = mock[UberStoreFileOps]
      val mockCodec = mock[OrderedEventCodec]

      val underTest = new UberDataFile(mockDesc, mockFileOps, mockCodec)

      // we will pretend we are starting at a later offset, and read a few events until we hit the end
      //  offset
      // Need to simulate 3 successful reads from the handle, corresponding with the offsets above
      val dummyBytes = "dummy".getBytes
      doReturn(Some(dummyBytes, 117L))
        .doReturn(Some(dummyBytes, 134L))
        .doReturn(Some(dummyBytes, 151L)).
        when(mockFileOps).readNext(any[RandomAccessFile], any[Long], any[Long])

      // Need to simulate the conversion of the events from above becoming OrderedEvents
      val event1 = OrderedEvent(1, 2, Delete("a"))
      val event2 = OrderedEvent(2, 3, Delete("b"))
      val event3 = OrderedEvent(3, 4, Delete("c"))
      doReturn(event1).doReturn(event2).doReturn(event3).
        when(mockCodec).deserialize(any[Array[Byte]])

      val result = underTest.foldLeftRange(100L, 134L)(List[(Long, OrderedEvent)]())(
        (acc, off, evt) => (off, evt) :: acc
      ).reverse

      assert(List((100L, event1), (117L, event2), (134L, event3)) === result)

      // verify that we started at the right offset
      verify(mockReadHandle).seek(100L)

      verify(mockFileOps, times(3)).readNext(same(mockReadHandle), any[Long], any[Long])
      verify(mockFileOps).readNext(mockReadHandle, 100L, 168L)
      verify(mockFileOps).readNext(mockReadHandle, 117L, 168L)
      verify(mockFileOps).readNext(mockReadHandle, 134L, 168L)
      verify(mockCodec, times(3)).deserialize(same(dummyBytes))

      // also verify cleanup!
      verify(mockReadHandle).close()
    }
  }

  describe ("close") {
    it ("should close provided writeHandle") {
      val mockWriteHandle = mock[RandomAccessFile]
      val mockDesc = mock[UberDataFile.UberFileDesc]
      doReturn(mockWriteHandle).when(mockDesc).createWriteHandle()

      val mockFileOps = mock[UberStoreFileOps]
      val mockCodec = mock[OrderedEventCodec]

      val underTest = new UberDataFile(mockDesc, mockFileOps, mockCodec)
      underTest.close()

      verify(mockWriteHandle).close()
    }
  }
}
