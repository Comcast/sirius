package com.comcast.xfinity.sirius.writeaheadlog

import com.comcast.xfinity.sirius.api.impl.OrderedEvent

/**
 * A WAL specific api for serializing and deserializing OrderedEvent,
 * for format understood by the Sirius append only file write ahead
 * log format
 */
trait WALSerDe {

  /**
   * Given raw serialized data as a String will convert it to a OrderedEvent
   */
  def deserialize(rawData: String): OrderedEvent

  /**
   * Given a OrderedEvent will serialize it to a String
   */
  def serialize(event: OrderedEvent): String
}