package com.comcast.xfinity.sirius.writeaheadlog

import java.util.regex.Pattern
import scala.collection.mutable.StringBuilder
import org.joda.time.DateTimeZone
import org.joda.time.format.{DateTimeFormatter, ISODateTimeFormat}

case class LogData(actionType: String, key: String, sequence: Long, timestamp: Long, payload: Array[Byte])

/**
 * Responsible for creating entries in the Sirius write ahead log.
 */
class WriteAheadLogEntry extends LogEntry with MD5Checksum with Base64PayloadCodec {
  val dateTimeFormatter: DateTimeFormatter = ISODateTimeFormat.basicDateTime()
  val whitespacePattern: Pattern = Pattern.compile("\\s")
  val pipePattern: Pattern = Pattern.compile("\\|")

  private[writeaheadlog] var logEntry: LogData = _

  /**
   * Creates a single log entry for the write ahead log.
   */
  def serialize(): String = {
    logEntry match {
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
        entryBuilder.append(encodePayload(payload))
        entryBuilder.append("|")
        entryBuilder.append(generateChecksum(entryBuilder.toString()))
        entryBuilder.append("\r")
        entryBuilder.toString()
      case _ => throw new IllegalStateException("No data to serialize. Must deserialize something first.")
    }
  }


  /**
   * Read a single log entry from a String.
   */
  def deserialize(rawData: String) {
    val Array(actionType, key, sequence, timestamp, payload, checksum) = rawData.split("\\|")
    logEntry = LogData(actionType, key, sequence.toLong, dateTimeFormatter.parseDateTime(timestamp).getMillis(), decodePayload(payload))
  }

  def validateKey(key: String) = {
    val whitespaceMatcher = whitespacePattern.matcher(key)
    val hasWhitespace = whitespaceMatcher.find()
    val pipeMatcher = pipePattern.matcher(key)
    val hasPipe = pipeMatcher.find()

    if (hasPipe) {
      throw new IllegalStateException("Key can't have | in it.")
    }
    if (hasWhitespace) {
      throw new IllegalStateException("Key can't have whitespace in it.")
    }

  }
}