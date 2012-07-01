package com.comcast.xfinity.sirius.writeaheadlog

import org.joda.time.format.ISODateTimeFormat
import org.slf4j.LoggerFactory

/**
 * Responsible for creating entries in the Sirius write ahead log.
 */
class WriteAheadLogSerDe extends LogDataSerDe with Checksum with Base64PayloadCodec {

  private val logger = LoggerFactory.getLogger(classOf[WriteAheadLogSerDe])
  
  val (formatTimestamp, parseTimestamp) = {
    val dateTimeFormatter = ISODateTimeFormat.basicDateTime()
    val doFormatTimestamp: Long => String = {
      val utcDateTimeFormatter = dateTimeFormatter.withZoneUTC()
      utcDateTimeFormatter.print(_)
    }
    val doParseTimestamp: String => Long = dateTimeFormatter.parseDateTime(_).getMillis
    (doFormatTimestamp, doParseTimestamp)
  }

  /**
   * Creates a single log entry for the write ahead log.
   */
  def serialize(logData: LogData): String = checksummedLogEntry(buildRawLogEntry(logData))

  /**
   * Retrieve a log entry from the write ahead log
   */
  def deserialize(rawData: String): LogData = {
    logger.debug("Raw Data: {}", rawData)
    logger.debug("data: {} -- {}", rawData.substring(24), rawData.substring(0, 24))
    validateAndDeserialize(rawData.substring(24), rawData.substring(0, 24))
  }


  private def checksummedLogEntry(base: String) =
    "%s%s".format(generateChecksum(base), base)

  private def buildRawLogEntry(data: LogData): String = {
    if (isKeyValid(data.key))
      "|%s|%s|%s|%s|%s\n".format(
        data.actionType,
        data.key,
        data.sequence,
        formatTimestamp(data.timestamp),
        encodePayload(data.payload))
    else
      throw new IllegalStateException("Key contains illegal character")
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

  def isKeyValid(key: String) = key.forall(c => !c.isWhitespace && c != '|') 
}
