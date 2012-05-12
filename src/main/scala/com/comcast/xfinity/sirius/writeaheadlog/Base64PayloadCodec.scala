package com.comcast.xfinity.sirius.writeaheadlog

import org.apache.commons.codec.binary.Base64

trait Base64PayloadCodec {

  private [writeaheadlog] var payloadCodec = new Base64();

  def encodePayload(payload: Array[Byte]): String = {
    payloadCodec.encodeToString(payload)
  }

  def decodePayload(payload: String): Array[Byte] = {
    payloadCodec.decode(payload.getBytes("utf-8"))
  }

}