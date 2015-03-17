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

package com.comcast.xfinity.sirius.uberstore.segmented

import com.comcast.xfinity.sirius.NiceTest
import org.mockito.Mockito._
import org.mockito.Matchers.{any, eq => meq, anyLong}
import com.comcast.xfinity.sirius.uberstore.data.UberDataFile
import com.comcast.xfinity.sirius.uberstore.seqindex.SeqIndex
import java.io.File
import org.scalatest.BeforeAndAfterAll
import scala.collection.immutable.StringOps
import com.comcast.xfinity.sirius.api.impl.{Put, Delete, OrderedEvent}
import scalax.file.Path

object SegmentTest {

  def createMockedUpLog: (UberDataFile, SeqIndex, FlagFile, FlagFile, Segment) = {
    val mockDataFile = mock(classOf[UberDataFile])
    val mockIndex = mock(classOf[SeqIndex])
    val mockFile = mock(classOf[File])
    val mockAppliedFlag = mock(classOf[FlagFile])
    val mockCompactedFlag = mock(classOf[FlagFile])
    // XXX: non-io tests require us to access private constructor
    val underTest =  new Segment(mockFile, "foo", mockDataFile, mockIndex, mockAppliedFlag, mockCompactedFlag)
    (mockDataFile, mockIndex, mockAppliedFlag, mockCompactedFlag, underTest)
  }
}

class SegmentTest extends NiceTest with BeforeAndAfterAll {

  import SegmentTest._

  val tempDir: File = {
    val tempDirName = "%s/segment-itest-%s".format(
      System.getProperty("java.io.tmpdir"),
      System.currentTimeMillis()
    )
    val dir = new File(tempDirName)
    dir.mkdirs()
    dir
  }

  override def afterAll() {
    Path(tempDir).deleteRecursively(force = true)
  }
  describe("writeEntry") {
    it ("must persist the event to the dataFile, and offset to the index") {
      val (mockDataFile, mockIndex, _, _, underTest) = createMockedUpLog

      doReturn(None).when(mockIndex).getMaxSeq
      doReturn(1234L).when(mockDataFile).writeEvent(any[OrderedEvent])

      val theEvent = OrderedEvent(5, 678, Delete("yarr"))
      underTest.writeEntry(theEvent)

      verify(mockDataFile).writeEvent(meq(theEvent))
      verify(mockIndex).put(meq(5L), meq(1234L))
    }

    it ("must not allow out of order events") {
      val (_, mockIndex, _, _, underTest) = createMockedUpLog

      doReturn(Some(5L)).when(mockIndex).getMaxSeq

      intercept[IllegalArgumentException] {
        underTest.writeEntry(OrderedEvent(1, 1234, Delete("FAIL")))
      }
    }
  }

  describe("getNextSeq") {
    it ("must return 1 if the index is empty") {
      val (_, mockIndex, _, _, underTest) = createMockedUpLog

      doReturn(None).when(mockIndex).getMaxSeq

      assert(1L === underTest.getNextSeq)
    }

    it ("must return 1 greater than the max if the index is populated") {
      val (_, mockIndex, _, _, underTest) = createMockedUpLog

      doReturn(Some(6L)).when(mockIndex).getMaxSeq

      assert(7L === underTest.getNextSeq)
    }
  }

  describe("foldLeft") {
    it ("must fold over the entire data file, as known by the index") {
      val (mockDataFile, mockIndex, _, _, underTest) = createMockedUpLog

      val theFoldFun = (s: Symbol, e: OrderedEvent) => s

      doReturn((0L, 1000L)).when(mockIndex).getOffsetRange(anyLong, anyLong)
      doReturn('orange).when(mockDataFile).foldLeftRange(anyLong, anyLong)(any[Symbol])(any[(Symbol, Long, OrderedEvent) => Symbol]())

      assert('orange === underTest.foldLeft('first)(theFoldFun))

      // Not really sure if there's a good way to verify this, the code is simple enough and this is tested
      //  by the integration test
      verify(mockDataFile).foldLeftRange(meq(0L), meq(1000L))(meq('first))(any[(Symbol, Long, OrderedEvent) => Symbol]())
    }
  }

  describe("isClosed") {
    val (mockDataFile, mockIndex, _, _, underTest) = createMockedUpLog
    it ("should return true if only index is closed") {
      doReturn(false).when(mockDataFile).isClosed
      doReturn(true).when(mockIndex).isClosed
      assert(true === underTest.isClosed)
    }
    it ("should return true if only datafile is closed") {
      doReturn(true).when(mockDataFile).isClosed
      doReturn(false).when(mockIndex).isClosed
      assert(true === underTest.isClosed)
    }
    it ("should return true if both index and datafile are closed") {
      doReturn(true).when(mockDataFile).isClosed
      doReturn(true).when(mockIndex).isClosed
      assert(true === underTest.isClosed)
    }
    it ("should return false if neither index nor datafile are closed") {
      doReturn(false).when(mockDataFile).isClosed
      doReturn(false).when(mockIndex).isClosed
      assert(false === underTest.isClosed)
    }
  }
  describe("size"){
    val (_, mockIndex, _, _, underTest) = createMockedUpLog
    it ("Should return the size of the index"){
      doReturn (2L).when(mockIndex).size
      assert(2 === underTest.size)

    }
  }
  describe ("keys"){

    it ("Should return an empty set if the set of keys are empty"){
      val underTest = Segment(tempDir, "0keys")
      assert(underTest.keys.isEmpty)
    }

    it ("Should return the correct number of keys if the set of keys is not empty. With Deletes only"){
      val underTest = Segment(tempDir, "hasKeys-Delete")
      underTest.writeEntry(OrderedEvent(1, 678, Delete("yarr")))
      underTest.writeEntry(OrderedEvent(2, 1200, Delete("secondYarr")))
      assert(2 === underTest.keys.size)
    }
    it ("Should return the correct number of keys if the set of keys is not empty. With Puts only"){
      val underTest = Segment(tempDir, "hasKeys-Put")
      val newByteArray = new StringOps("data").getBytes
      underTest.writeEntry(OrderedEvent(1, 678, Put("yarr",newByteArray)))
      underTest.writeEntry(OrderedEvent(2, 1000, Put("secondYarr",newByteArray)))
      assert(Set("yarr", "secondYarr") === underTest.keys)
    }
    it ("Should return the correct number of keys if the set of keys is not empty. With Puts & Deletes"){
      val underTest = Segment(tempDir, "hasKeys-All")
      val newByteArray = new StringOps("data").getBytes
      underTest.writeEntry(OrderedEvent(1, 678, Put("yarr",newByteArray)))
      underTest.writeEntry(OrderedEvent(2, 1000, Put("secondYarr",newByteArray)))
      underTest.writeEntry(OrderedEvent(3, 1100, Delete("thirdYarr")))
      underTest.writeEntry(OrderedEvent(4, 1200, Delete("fourthYarr")))
      assert(Set("yarr", "secondYarr", "thirdYarr", "fourthYarr") === underTest.keys)
    }
    it ("should return a unique number of keys if the set of keys is not empty and has duplicates"){
      val underTest = Segment(tempDir, "hasUniqueKeys")
      val newByteArray = new StringOps("data").getBytes
      underTest.writeEntry(OrderedEvent(1, 678, Delete("yarr")))
      underTest.writeEntry(OrderedEvent(2, 1200, Delete("secondYarr")))
      underTest.writeEntry(OrderedEvent(3, 1300, Delete("secondYarr")))
      underTest.writeEntry(OrderedEvent(4, 1400, Put("thirdYarr",newByteArray)))
      underTest.writeEntry(OrderedEvent(5, 1500, Put("thirdYarr",newByteArray)))
      assert(Set("yarr", "secondYarr", "thirdYarr") === underTest.keys)
    }
  }
  describe("close") {
    it ("should close underlying index and data") {
      val (mockDataFile, mockIndex, _, _, underTest) = createMockedUpLog
      doReturn(false).when(mockDataFile).isClosed
      doReturn(false).when(mockIndex).isClosed

      underTest.close()

      verify(mockDataFile).close()
      verify(mockIndex).close()
    }
  }
  describe("isApplied") {
    it ("should call through to the underlying FlagFiles") {
      val (_, _, mockFlag, mockFlag2, underTest) = createMockedUpLog
      doReturn(true).when(mockFlag).value
      doReturn(false).when(mockFlag2).value

      assert(true === underTest.isApplied)
      assert(false === underTest.isInternallyCompacted)
      verify(mockFlag).value
      verify(mockFlag2).value
    }
  }
  describe("setApplied") {
    it ("should call through to the underlying FlagFiles") {
      val (_, _, mockFlag, mockFlag2, underTest) = createMockedUpLog

      underTest.setApplied(applied = true)
      underTest.setInternallyCompacted(compacted = false)
      verify(mockFlag).set(value = true)
      verify(mockFlag2).set(value = false)
    }
  }
}
