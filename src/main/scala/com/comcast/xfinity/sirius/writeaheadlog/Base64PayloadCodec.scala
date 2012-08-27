package com.comcast.xfinity.sirius.writeaheadlog

import org.apache.commons.codec.binary.Base64
import org.apache.commons.lang.StringUtils

trait Base64PayloadCodec {

  // XXX: needs to be a def, Base64 is not thread safe, but it is used in a non-thread-safe manner
  private def payloadCodec = new Base64()

  def encodePayload(payload: Option[Array[Byte]]): String = payloadCodec.encodeToString(payload.getOrElse("".getBytes))

  def decodePayload(payload: String): Option[Array[Byte]] = {
    if (StringUtils.isEmpty(payload)) {
      None
    } else {
      Some(payloadCodec.decode(payload.getBytes("utf-8")))
    }
  }

}