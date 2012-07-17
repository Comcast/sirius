package com.comcast.xfinity.sirius.writeaheadlog

import org.joda.time.format.ISODateTimeFormat
import com.comcast.xfinity.sirius.api.impl.{Delete, Put, NonCommutativeSiriusRequest, OrderedEvent}

/**
 * Responsible for creating entries in the Sirius write ahead log.
 */
class WriteAheadLogSerDe extends WALSerDe with Checksum with Base64PayloadCodec {

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
    validateAndDeserialize(rawData.substring(CHECKSUM_LENGTH), rawData.substring(0, CHECKSUM_LENGTH))
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
  private def extractFields(request: NonCommutativeSiriusRequest) = request match {
    case Put(key, body) => ("PUT", key, Some(body))
    case Delete(key) => ("DELETE", key, None)
  }

  private def validateAndDeserialize(data: String, checksum: String) = {
    if (validateChecksum(data, checksum))
      orderedEventOfString(data)
    else
      throw new SiriusChecksumException("Checksum does not match for entry: \n" + checksum +  data)
  }

  private def orderedEventOfString(str: String) = str.split("\\|") match {
    case Array("", "PUT", key, seq, ts, payload) =>
      // XXX: payload may not be None
      OrderedEvent(seq.toLong, parseTimestamp(ts), Put(key, decodePayload(payload).get))
    case Array("", "DELETE", key, seq, ts, _) =>
      OrderedEvent(seq.toLong, parseTimestamp(ts), Delete(key))
  }

  def isKeyValid(key: String) = key.forall(c => !c.isWhitespace && c != '|') 
}
