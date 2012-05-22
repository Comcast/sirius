package com.comcast.xfinity.sirius.writeaheadlog

import java.util.regex.Pattern
import scala.collection.mutable.StringBuilder
import org.joda.time.DateTimeZone
import org.joda.time.format.{DateTimeFormatter, ISODateTimeFormat}

case class LogData(actionType: String, key: String, sequence: Long, timestamp: Long, payload: Array[Byte])

/**
 * Responsible for creating entries in the Sirius write ahead log.
 */
class WriteAheadLogEntry extends LogEntry with Checksum with Base64PayloadCodec {
  val dateTimeFormatter: DateTimeFormatter = ISODateTimeFormat.basicDateTime()
  val whitespacePattern: Pattern = Pattern.compile("\\s")
  val pipePattern: Pattern = Pattern.compile("\\|")

  private[writeaheadlog] var logData: LogData = _

  /**
   * Creates a single log entry for the write ahead log.
   */
  def serialize(): String = {
    val rawLogEntry = buildRawLogEntry(logData)
    "%s%s\r".format(rawLogEntry, generateChecksum(rawLogEntry))
  }

  private def buildRawLogEntry(data: LogData): String = {
    data match {
      case LogData(actionType, key, sequence, timestamp, payload) =>
        validateKey(key)
        "%s|%s|%s|%s|%s|".format(
            actionType,
            key,
            sequence,
            dateTimeFormatter.withZone(DateTimeZone.UTC).print(timestamp),
            encodePayload(payload))
      case _ => throw new IllegalStateException("No data to serialize. Must deserialize something first.")
    }
  }

  /**
   * Validate a checksum
   */
  private def validateChecksum(data: LogData, checksum: String) {

    val dataToBeChecksumed = buildRawLogEntry(data)

      if(!validateChecksum(dataToBeChecksumed, checksum)) {
        throw new SiriusChecksumException("Checksum does not match.")
      }
  }


  private def cleanChecksum(dirtyChecksum: String): String = {
    val parts: Array[String] = dirtyChecksum.split("\r")
    if (parts.size > 2) {
      throw new IllegalArgumentException("Checksum is dirtier than expected: " + dirtyChecksum)
    }
    parts(0)
  }


  def deserialize(rawData: String) {
    val Array(actionType, key, sequence, timestamp, payload, checksum) = rawData.split("\\|")
    logData = LogData(actionType, key, sequence.toLong, dateTimeFormatter.parseDateTime(timestamp).getMillis(), decodePayload(payload))

    validateChecksum(logData, cleanChecksum(checksum))
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