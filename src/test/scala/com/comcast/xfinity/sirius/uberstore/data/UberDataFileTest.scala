package com.comcast.xfinity.sirius.uberstore.data

import com.comcast.xfinity.sirius.NiceTest
import java.io.RandomAccessFile
import com.comcast.xfinity.sirius.uberstore.{UberStoreFileOps, OrderedEventCodec}
import org.mockito.Mockito._
import com.comcast.xfinity.sirius.api.impl.{OrderedEvent, Delete}
import org.mockito.Matchers.{any, same}

class UberDataFileTest extends NiceTest {

  it ("must seek to the end of the write file handle on instantiation") {
    val mockWriteHandle = mock[RandomAccessFile]
    val mockReadHandle = mock[RandomAccessFile]
    val mockFileOps = mock[UberStoreFileOps]
    val mockCodec = mock[OrderedEventCodec]

    doReturn(100L).when(mockWriteHandle).length

    new UberDataFile("fname", mockFileOps, mockCodec) with UberDataFile.HandleProvider {
      def createWriteHandle(fname: String) = mockWriteHandle
      def createReadHandle(fname: String) = mockReadHandle
    }

    verify(mockWriteHandle).seek(100L)
  }

  describe("writeEvent") {
    it ("must serialize the event and delegate its persistence to fileOps") {
      val mockWriteHandle = mock[RandomAccessFile]
      val mockReadHandle = mock[RandomAccessFile]
      val mockFileOps = mock[UberStoreFileOps]
      val mockCodec = mock[OrderedEventCodec]

      val underTest = new UberDataFile("fname", mockFileOps, mockCodec) with UberDataFile.HandleProvider {
        def createWriteHandle(fname: String) = mockWriteHandle
        def createReadHandle(fname: String) = mockReadHandle
      }

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
      val mockFileOps = mock[UberStoreFileOps]
      val mockCodec = mock[OrderedEventCodec]

      val underTest = new UberDataFile("fname", mockFileOps, mockCodec) with UberDataFile.HandleProvider {
        def createWriteHandle(fname: String) = mockWriteHandle
        def createReadHandle(fname: String) = mockReadHandle
      }

      // Need to simulate 3 successful reads from the handle, followed by None indicating we hit the end
      val dummyBytes = "dummy".getBytes
      doReturn(Some(dummyBytes)).doReturn(Some(dummyBytes)).doReturn(Some(dummyBytes)).doReturn(None).
        when(mockFileOps).readNext(any[RandomAccessFile])

      // Need to simulate the conversion of the events from above becoming OrderedEvents
      val event1 = OrderedEvent(1, 2, Delete("a"))
      val event2 = OrderedEvent(2, 3, Delete("b"))
      val event3 = OrderedEvent(3, 4, Delete("c"))
      doReturn(event1).doReturn(event2).doReturn(event3).
        when(mockCodec).deserialize(any[Array[Byte]])

      val result = underTest.foldLeft(List[OrderedEvent]())((a, e) => e :: a)

      assert(List(event3, event2, event1) === result)

      // verify that we read and deserialized the expected number of times
      verify(mockFileOps, times(4)).readNext(same(mockReadHandle))
      verify(mockCodec, times(3)).deserialize(same(dummyBytes))

      // also verify cleanup!
      verify(mockReadHandle).close()
    }
  }

  describe("foldLeftRange") {
    it ("must only iterate over the specified range of offsets, inclusive, and when finished close the handle") {
      val mockWriteHandle = mock[RandomAccessFile]
      val mockReadHandle = mock[RandomAccessFile]
      val mockFileOps = mock[UberStoreFileOps]
      val mockCodec = mock[OrderedEventCodec]

      val underTest = new UberDataFile("fname", mockFileOps, mockCodec) with UberDataFile.HandleProvider {
        def createWriteHandle(fname: String) = mockWriteHandle
        def createReadHandle(fname: String) = mockReadHandle
      }

      // we will pretend we are starting at a later offset, and read a few events until we hit the end
      //  offset
      doReturn(100L).doReturn(110L).doReturn(120L).doReturn(130L).
        when(mockReadHandle).getFilePointer

      // Need to simulate 3 successful reads from the handle, corresponding with the offsets above
      val dummyBytes = "dummy".getBytes
      doReturn(Some(dummyBytes)).doReturn(Some(dummyBytes)).doReturn(Some(dummyBytes)).
        when(mockFileOps).readNext(any[RandomAccessFile])

      // Need to simulate the conversion of the events from above becoming OrderedEvents
      val event1 = OrderedEvent(1, 2, Delete("a"))
      val event2 = OrderedEvent(2, 3, Delete("b"))
      val event3 = OrderedEvent(3, 4, Delete("c"))
      doReturn(event1).doReturn(event2).doReturn(event3).
        when(mockCodec).deserialize(any[Array[Byte]])

      val result = underTest.foldLeftRange(100L, 120L)(List[OrderedEvent]())((a, e) => e :: a)

      assert(List(event3, event2, event1) === result)

      // verify that we started at the right offset
      verify(mockReadHandle).seek(100L)

      verify(mockFileOps, times(3)).readNext(same(mockReadHandle))
      verify(mockCodec, times(3)).deserialize(same(dummyBytes))

      // also verify cleanup!
      verify(mockReadHandle).close()
    }
  }
}