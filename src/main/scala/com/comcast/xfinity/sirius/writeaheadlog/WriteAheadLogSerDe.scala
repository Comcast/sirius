package com.comcast.xfinity.sirius.writeaheadlog

import java.util.regex.Pattern
import org.joda.time.DateTimeZone
import org.joda.time.format.{ DateTimeFormatter, ISODateTimeFormat }

/**
 * Responsible for creating entries in the Sirius write ahead log.
 */
class WriteAheadLogSerDe extends LogDataSerDe with Checksum with Base64PayloadCodec {
  val whitespacePattern: Pattern = Pattern.compile("\\s")
  val pipePattern: Pattern = Pattern.compile("\\|")

  val (formatTimestamp, parseTimestamp) = {
    val dateTimeFormatter = ISODateTimeFormat.basicDateTime()
    val doFormatTimestamp: Long => String = {
      val utcDateTimeFormatter = dateTimeFormatter.withZoneUTC()
      utcDateTimeFormatter.print(_)
    }
    val doParseTimestamp: String => Long = dateTimeFormatter.parseDateTime(_).getMillis()
    (doFormatTimestamp, doParseTimestamp)
  }

  /**
   * Creates a single log entry for the write ahead log.
   */
  def serialize(logData: LogData): String = checksummedLogEntry(buildRawLogEntry(logData))

  private def checksummedLogEntry(base: String) =
    "%s%s".format(generateChecksum(base), base)

  private def buildRawLogEntry(data: LogData): String = {
    validateKey(data.key)
    "|%s|%s|%s|%s|%s\n".format(
      data.actionType,
      data.key,
      data.sequence,
      formatTimestamp(data.timestamp),
      encodePayload(data.payload))
  }

  /**
   * Validate a checksum
   */
  private def validateChecksum(data: LogData, checksum: String) {

    val dataToBeChecksumed = buildRawLogEntry(data)

    if (!validateChecksum(dataToBeChecksumed, checksum)) {
      throw new SiriusChecksumException("Checksum does not match.")
    }
  }

  def deserialize(rawData: String): LogData = {
    val Array(checksum, actionType, key, sequence, timestamp, payload) = rawData.split("\\|")
    val data = LogData(actionType, key, sequence.toLong, parseTimestamp(timestamp), decodePayload(payload))
    validateChecksum(data, checksum)
    data
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