package com.comcast.xfinity.sirius.writeaheadlog

/**
 * Represents an entry in a write ahead log
 */
case class LogData(actionType: String, key: String, sequence: Long, timestamp: Long, payload: Option[Array[Byte]])