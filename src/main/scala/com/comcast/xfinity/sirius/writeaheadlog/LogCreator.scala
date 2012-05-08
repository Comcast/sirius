package com.comcast.xfinity.sirius.writeaheadlog

import scala.collection.mutable.StringBuilder

import org.apache.commons.codec.binary.Base64
import org.joda.time.format.DateTimeFormatter
import org.joda.time.format.ISODateTimeFormat

case class LogData(actionType : String, key : String, sequence : Long, timestamp : Long, payload : Array[Byte])

class LogCreator {  
  val dateTimeFormatter = ISODateTimeFormat.basicDateTime()
  
  def createLogEntry(entryData : LogData) : String = {
    entryData match { 
      case LogData(actionType, key, sequence, timestamp, payload) =>
        val entryBuilder = new StringBuilder()
        entryBuilder.append(actionType)
        entryBuilder.append("|")
        entryBuilder.append(key)
        entryBuilder.append("|")
        entryBuilder.append(sequence)
        entryBuilder.append("|")
        entryBuilder.append(dateTimeFormatter.print(timestamp))
        entryBuilder.append("|")
        entryBuilder.append(Base64.encodeBase64String(payload))
        entryBuilder.toString()
    }
  } 
  
}