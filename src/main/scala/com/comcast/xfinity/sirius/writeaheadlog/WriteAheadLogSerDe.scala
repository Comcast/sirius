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

  /**
   * Retrieve a log entry from the write ahead log
   */
  def deserialize(rawData: String): LogData = 
    validateAndDeserialize(rawData.substring(24), rawData.substring(0, 24))

  
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
  
  private def validateAndDeserialize(data: String, checksum: String) = {
    if (validateChecksum(data, checksum))
      logDataOfString(data)
    else 
      throw new SiriusChecksumException("Checksum does not match.")
  }
  
  private def logDataOfString(logDataString: String) = logDataString.split("\\|") match {
    case Array("", action, key, seq, ts, payload) =>
      LogData(action, key, seq.toLong, parseTimestamp(ts), decodePayload(payload))
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