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
import java.lang.Object
import java.util.Arrays
import com.comcast.xfinity.sirius.api.impl.{Delete, Put, OrderedEvent}
import java.nio.{ByteOrder, ByteBuffer}

class BinaryEventCodecTest extends NiceTest {

  val underTest = new BinaryEventCodec

  describe("serialize") {
    it ("must properly serialize Puts and be the inverse of deserialize") {
      val key = "hello"
      val body = "world".getBytes

      val expected = ByteBuffer.allocate(8 + 8 + 4 + 4 + 4 + key.getBytes.length + body.length)

      expected.order(ByteOrder.BIG_ENDIAN)
      expected.putLong(1234)
      expected.putLong(5678)
      expected.putInt(1)
      expected.putInt(key.getBytes.length)
      expected.putInt(body.length)
      expected.put(key.getBytes)
      expected.put(body)

      val orderedPut = OrderedEvent(1234, 5678, Put(key, body))
      val serializedPut = underTest.serialize(orderedPut)
      assert(Arrays.equals(expected.array, serializedPut),
        "Put did not serialize as expected")

      val deserializedPut = underTest.deserialize(serializedPut)
      assert(orderedPut === deserializedPut,
        "deserialize was not the inverse of serialize for Put")
    }

    it ("must properly serialize Deletes and be the inverse of serialize") {
      val key = "hello"

      val expected = ByteBuffer.allocate(8 + 8 + 4 + 4 + 4 + key.getBytes.length + 0)

      expected.order(ByteOrder.BIG_ENDIAN)
      expected.putLong(1234)
      expected.putLong(5678)
      expected.putInt(2)
      expected.putInt(key.getBytes.length)
      expected.putInt(0)
      expected.put(key.getBytes)

      val orderedDelete = OrderedEvent(1234, 5678, Delete(key))
      val serializedDelete = underTest.serialize(orderedDelete)
      assert(Arrays.equals(expected.array, serializedDelete),
        "Delete did not serialize as expected")

      val deserializedDelete = underTest.deserialize(serializedDelete)
      assert(orderedDelete === deserializedDelete,
        "deserialize was not the inverse of serialize for Delete")
    }
  }

  describe("deserialize") {
    it ("must properly deserialize Puts and be the inverse of serialize") {
      val key = "hello"
      val body = "world".getBytes

      val expected = OrderedEvent(1234, 5678, Put(key, body))

      val putBytesBuf = ByteBuffer.allocate(8 + 8 + 4 + 4 + 4 + key.getBytes.length + body.length)

      putBytesBuf.order(ByteOrder.BIG_ENDIAN)
      putBytesBuf.putLong(1234)
      putBytesBuf.putLong(5678)
      putBytesBuf.putInt(1)
      putBytesBuf.putInt(key.getBytes.length)
      putBytesBuf.putInt(body.length)
      putBytesBuf.put(key.getBytes)
      putBytesBuf.put(body)

      val deserializedPut = underTest.deserialize(putBytesBuf.array)
      assert(expected === deserializedPut,
        "Put did not deserialize as expected")

      val serializedPut = underTest.serialize(deserializedPut)
      assert(Arrays.equals(putBytesBuf.array, serializedPut),
        "serialize was not the inverse of deserialize for Put")
    }

    it ("must properly deserialize Deletes and be the inverse of deserialize") {
      val key = "hello"

      val expected = OrderedEvent(1234, 5678, Delete(key))

      val putBytesBuf = ByteBuffer.allocate(8 + 8 + 4 + 4 + 4 + key.getBytes.length)

      putBytesBuf.order(ByteOrder.BIG_ENDIAN)
      putBytesBuf.putLong(1234)
      putBytesBuf.putLong(5678)
      putBytesBuf.putInt(2)
      putBytesBuf.putInt(key.getBytes.length)
      putBytesBuf.putInt(0)
      putBytesBuf.put(key.getBytes)

      val deserializedDelete = underTest.deserialize(putBytesBuf.array)
      assert(expected === deserializedDelete,
        "Delete did not deserialize as expected")

      val serializedDelete = underTest.serialize(deserializedDelete)
      assert(Arrays.equals(putBytesBuf.array, serializedDelete),
        "serialize was not the inverse of deserialize for Delete")
    }
  }

}
