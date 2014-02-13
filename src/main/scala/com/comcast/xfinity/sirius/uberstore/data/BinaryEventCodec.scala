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

import com.comcast.xfinity.sirius.api.impl.{Delete, Put, OrderedEvent}
import java.nio.ByteBuffer

/**
 * Class supplying binary encoding of OrderedEvents
 */
class BinaryEventCodec extends OrderedEventCodec {

  private final val PUT_CODE: Int = 1
  private final val DELETE_CODE: Int = 2

  private final val EMPTY_BYTES = new Array[Byte](0)

  /**
   * @inheritdoc
   */
  def serialize(event: OrderedEvent): Array[Byte] = {
    val typeCode = typeCodeFromOrderedEvent(event)

    val key = keyFromOrderedEvent(event).getBytes
    val body = bodyFromOrderedEvent(event)

    val keyLen = key.length
    val bodyLen = body.length

    val eventBuf = ByteBuffer.allocate(
      8 + // seq
      8 + // timestamp
      4 + // typeCode
      4 + // keyLen
      4 + // bodyLen
      keyLen + // key data
      bodyLen // bodyData
    )

    eventBuf.putLong(event.sequence).
      putLong(event.timestamp).
      putInt(typeCode).
      putInt(keyLen).
      putInt(bodyLen).
      put(key).
      put(body).array
  }

  /**
   * @inheritdoc
   */
  def deserialize(bytes: Array[Byte]): OrderedEvent = {
    val eventBuf = ByteBuffer.wrap(bytes)

    val seq = eventBuf.getLong()
    val timestamp = eventBuf.getLong()
    val typeCode = eventBuf.getInt()
    val keyLen = eventBuf.getInt()
    val bodyLen = eventBuf.getInt()

    val keyBytes = new Array[Byte](keyLen)
    eventBuf.get(keyBytes)
    val key = new String(keyBytes)

    val body = new Array[Byte](bodyLen)
    eventBuf.get(body)

    val req = if (typeCode == PUT_CODE) Put(key, body) else Delete(key)
    OrderedEvent(seq, timestamp, req)
  }


  private def typeCodeFromOrderedEvent(event: OrderedEvent): Int = event.request match {
    case _: Put => PUT_CODE
    case _: Delete => DELETE_CODE
  }

  private def keyFromOrderedEvent(event: OrderedEvent): String = event.request match {
    case Put(key, _) => key
    case Delete(key) => key
  }

  private def bodyFromOrderedEvent(event: OrderedEvent): Array[Byte] = event.request match {
    case Put(_, body) => body
    case _: Delete => EMPTY_BYTES
  }
}
