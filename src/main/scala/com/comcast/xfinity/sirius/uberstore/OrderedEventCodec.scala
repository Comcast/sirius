package com.comcast.xfinity.sirius.uberstore

import com.comcast.xfinity.sirius.api.impl.OrderedEvent

/**
 * Trait supplying functions for converting OrderedEvents to and
 * from Array[Byte]s
 */
trait OrderedEventCodec {
  /**
   * Serialize event to Array[Byte]
   *
   * @param event the OrderedEvent to serialize
   *
   * @return the serialized OrderedEvent
   */
  def serialize(event: OrderedEvent): Array[Byte]

  /**
   * Read an OrderedEvent from an Array[Byte]
   *
   * If you give it a bad event, bad things will happen, don't be that guy
   *
   * @param bytes the Array[Byte] to decode
   *
   * @return the OrderedEvent decoded from bytes
   */
  def deserialize(bytes: Array[Byte]): OrderedEvent
}