package com.comcast.xfinity.sirius.writeaheadlog

/**
 * Represents an entry in a write ahead log
 */
case class LogData(actionType: String, key: String, sequence: Long, timestamp: Long, payload: Option[Array[Byte]]) {
  
  override def hashCode = (key + sequence).hashCode()
  
  override def equals(obj: Any) : Boolean = {
    obj.isInstanceOf[LogData] && (this.hashCode == obj.asInstanceOf[LogData].hashCode)
  }
}