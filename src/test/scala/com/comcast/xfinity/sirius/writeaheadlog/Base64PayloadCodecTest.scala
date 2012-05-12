package com.comcast.xfinity.sirius.writeaheadlog

import org.scalatest.{BeforeAndAfter, FunSpec}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class Base64PayloadCodecTest extends FunSpec with BeforeAndAfter {

  var codec: Base64PayloadCodec = _


  before {
    codec = new Base64PayloadCodec() {}
  }

  describe("a Base64PayloadCodec") {
    it("should encode and decode to the same thing") {
      val payload = "something to encode"
      payload.equals(codec.decodePayload(codec.encodePayload(payload.getBytes("utf-8"))))
    }

    it ("should decode and encode to the same thing") {
      val encodedPayload = "encoded payload %@#$@#$"
      encodedPayload.equals(codec.encodePayload(codec.decodePayload(encodedPayload)))
    }
  }

}