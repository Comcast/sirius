package com.comcast.xfinity.sirius.writeaheadlog

import java.security.MessageDigest
import java.util.regex.Matcher
import java.util.regex.Pattern
import scala.collection.mutable.StringBuilder
import org.apache.commons.codec.binary.Base64
import org.joda.time.format.DateTimeFormatter
import org.joda.time.format.ISODateTimeFormat
import org.joda.time.DateTimeZone

case class LogData(actionType: String, key: String, sequence: Long, timestamp: Long, payload: Array[Byte])

/**
 * Responsible for creating entries in the Sirius write ahead log.
 */
class WriteAheadLog {
  val dateTimeFormatter = ISODateTimeFormat.basicDateTime()
  val md5Digest = MessageDigest.getInstance("MD5");
  val whitespacePattern = Pattern.compile("\\s")
  val pipePattern = Pattern.compile("\\|")
  
  /**
   * Creates a single log entry for the write ahead log.
   */
  def createLogEntry(entryData: LogData): String = {
    entryData match {
      case LogData(actionType, key, sequence, timestamp, payload) =>
        validateKey(key)

        val entryBuilder = new StringBuilder()
        entryBuilder.append(actionType)
        entryBuilder.append("|")
        entryBuilder.append(key)
        entryBuilder.append("|")
        entryBuilder.append(sequence)
        entryBuilder.append("|")
        entryBuilder.append(dateTimeFormatter.withZone(DateTimeZone.UTC).print(timestamp))
        entryBuilder.append("|")
        entryBuilder.append(Base64.encodeBase64String(payload))
        entryBuilder.append("|")
        val bytesSoFar = entryBuilder.toString().getBytes("US-ASCII")
        entryBuilder.append(Base64.encodeBase64String(md5Digest.digest(bytesSoFar)))
        entryBuilder.append("\r")

        entryBuilder.toString()
    }
  }
  
  /**
   * Read a single log entry from a String.
   */
  def parseLogEntry(logEntry : String): LogData = {
    val Array(actionType, key, sequence, timestamp, payload, checksum) = logEntry.split("\\|")
    
    
    return LogData(actionType, key, sequence.toLong,dateTimeFormatter.parseDateTime(timestamp).getMillis(), Base64.decodeBase64(payload))
  }
  
  def validateKey(key: String) = {
    val whitespaceMatcher = whitespacePattern.matcher(key)
    val hasWhitespace = whitespaceMatcher.find()
    val pipeMatcher = pipePattern.matcher(key)
    val hasPipe = pipeMatcher.find()
    
    if(hasPipe)
      throw new IllegalStateException("Key can't have | in it.")
    if(hasWhitespace)
      throw new IllegalStateException("Key can't have whitespace in it.")
    
  }
}