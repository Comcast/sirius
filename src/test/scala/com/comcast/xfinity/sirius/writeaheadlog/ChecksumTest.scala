package com.comcast.xfinity.sirius.writeaheadlog

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.mockito.Mockito._
import java.security.MessageDigest
import org.apache.commons.codec.binary.Base64
import com.comcast.xfinity.sirius.NiceTest

@RunWith(classOf[JUnitRunner])
class ChecksumTest extends NiceTest {

  var checksumGenerator: Checksum = _

  var mockMessageDigest: MessageDigest = _
  var mockCodec: Base64 = _

  val HASH = "some hash".getBytes()
  val ENCODED_HASH = "some encoded hash"
  val DATA = "some thing to checksum"


  before {
    mockCodec = mock[Base64]
    mockMessageDigest = mock[MessageDigest]
    checksumGenerator = new ChecksumForTesting(mockMessageDigest)
    checksumGenerator.checksumCodec = mockCodec

  }

  describe("An MD5Checksum") {
    it("should generate a checksum that verfies") {
      when(mockMessageDigest.digest(DATA.getBytes())).thenReturn(HASH)
      when(mockCodec.encodeToString(HASH)).thenReturn(ENCODED_HASH)

      val checksum = checksumGenerator.generateChecksum(DATA)

      assert(checksum === ENCODED_HASH)
      assert(checksumGenerator.validateChecksum(DATA, checksum))
    }


    it("should not verify a tampered with checksum") {
      var checksum = checksumGenerator.generateChecksum(DATA)
      checksum += "ggg"

      assert(!checksumGenerator.validateChecksum(DATA, checksum))
    }


  }

  class ChecksumForTesting(mD: MessageDigest) extends Checksum {
    override def getMessageDigest(): MessageDigest = {
      mD
    }

  }

}

