package com.comcast.xfinity.sirius.writeaheadlog




 trait LogEntry {
  def deserialize(rawData: String)

  def serialize(): String
}