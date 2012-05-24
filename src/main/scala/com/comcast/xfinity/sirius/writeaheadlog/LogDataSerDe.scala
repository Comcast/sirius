package com.comcast.xfinity.sirius.writeaheadlog

/**
 * An api for serializing and deserializing LogData
 */
trait LogDataSerDe {

  /**
   * Given raw serialized data as a String will convert it to a LogData
   */
  def deserialize(rawData: String): LogData

  /**
   * Given a LogData will serialize it to a String
   */
  def serialize(logData: LogData): String
}