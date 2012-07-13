package com.comcast.xfinity.sirius.writeaheadlog

import org.joda.time.format.ISODateTimeFormat
import org.slf4j.LoggerFactory
import com.comcast.xfinity.sirius.api.impl.{Delete, Put, NonIdempotentSiriusRequest, OrderedEvent}

/**
 * Responsible for creating entries in the Sirius write ahead log.
 */
class WriteAheadLogSerDe extends WALSerDe with Checksum with Base64PayloadCodec {

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
  def serialize(event: OrderedEvent): String = checksummedLogEntry(buildRawLogEntry(event))

  /**
   * Retrieve a log entry from the write ahead log
   */
  def deserialize(rawData: String): OrderedEvent = {
    logger.debug("Raw Data: {}", rawData)
    logger.debug("data: {} -- {}", rawData.substring(24), rawData.substring(0, 24))
    validateAndDeserialize(rawData.substring(24), rawData.substring(0, 24))
  }

  private def checksummedLogEntry(base: String) =
    "%s%s".format(generateChecksum(base), base)

  private def buildRawLogEntry(event: OrderedEvent): String = {
    val (action, key, payload) = extractFields(event.request)
    if (isKeyValid(key))
      "|%s|%s|%s|%s|%s\n".format(
        action,
        key,
        event.sequence,
        formatTimestamp(event.timestamp),
        encodePayload(payload))
    else
      throw new IllegalStateException("Key contains illegal character")
  }

  // XXX: here for transition
  private def extractFields(request: NonIdempotentSiriusRequest) = request match {
    case Put(key, body) => ("PUT", key, Some(body))
    case Delete(key) => ("DELETE", key, None)
  }

  private def validateAndDeserialize(data: String, checksum: String) = {
    if (validateChecksum(data, checksum))
      orederedEventOfString(data)
    else
      throw new SiriusChecksumException("Checksum does not match for entry: \n" + checksum +  data)
  }

  private def orederedEventOfString(str: String) = str.split("\\|") match {
    case Array("", "PUT", key, seq, ts, payload) =>
      // XXX: payload may not be None
      OrderedEvent(seq.toLong, parseTimestamp(ts), Put(key, decodePayload(payload).get))
    case Array("", "DELETE", key, seq, ts, _) =>
      OrderedEvent(seq.toLong, parseTimestamp(ts), Delete(key))
  }

  def isKeyValid(key: String) = key.forall(c => !c.isWhitespace && c != '|') 
}
