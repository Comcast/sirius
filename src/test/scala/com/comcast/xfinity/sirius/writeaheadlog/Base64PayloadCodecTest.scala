package com.comcast.xfinity.sirius.writeaheadlog

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import com.comcast.xfinity.sirius.NiceTest

@RunWith(classOf[JUnitRunner])
class Base64PayloadCodecTest extends NiceTest {

  var codec: Base64PayloadCodec = _


  before {
    codec = new Base64PayloadCodec() {}
  }

  describe("a Base64PayloadCodec") {
    it("should encode and decode to the same thing") {
      val payload = Some("something to encode".getBytes("utf-8"))
      payload.equals(codec.decodePayload(codec.encodePayload(payload)))
    }

    it ("should decode and encode to the same thing") {
      val encodedPayload = "encoded payload %@#$@#$"
      encodedPayload.equals(codec.encodePayload(codec.decodePayload(encodedPayload)))
    }
  }

}