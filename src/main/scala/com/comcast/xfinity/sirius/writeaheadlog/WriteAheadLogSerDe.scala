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

  // note that quickSplit produces a backwards list
  private def orderedEventOfString(str: String) = (quickSplit(str, '|'): @unchecked) match {
    case List(payload, ts, seq, key, "PUT", "") =>
      // XXX: payload may not be None
      OrderedEvent(seq.toLong, parseTimestamp(ts), Put(key, decodePayload(payload).get))
    case List(_, ts, seq, key, "DELETE", "") =>
      OrderedEvent(seq.toLong, parseTimestamp(ts), Delete(key))
  }

  // Significantly more performant version of split for splitting on a single char
  // returns the resultant splits in reverse order, we could reverse this, but this
  // function is single use, and on a pretty intense performance path
  private def quickSplit(subject: String,
                 delim: Int,
                 accum: List[String] = Nil): List[String] =
    subject.indexOf(delim) match {
      case -1 => subject :: accum
      case idx =>
        quickSplit(
            subject.substring(idx + 1),
            delim,
            subject.substring(0, idx) :: accum
        )
    }


  def isKeyValid(key: String) = key.forall(c => !c.isWhitespace && c != '|') 
}
