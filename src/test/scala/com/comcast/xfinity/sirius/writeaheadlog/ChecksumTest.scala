package com.comcast.xfinity.sirius.writeaheadlog

import com.comcast.xfinity.sirius.NiceTest

class ChecksumTest extends NiceTest {

  var checksumGenerator: Checksum = _

  val CHECKSUM = "d96793d7c6ee3d15"
  val DATA = "some thing to checksum"


  before {
    checksumGenerator = new Object with Checksum
  }

  describe("A FNV-1 Checksum") {
    it("should generate a checksum that verfies") {
      val checksum = checksumGenerator.generateChecksum(DATA)

      assert(checksum === CHECKSUM)
      assert(checksumGenerator.validateChecksum(DATA, checksum))
    }

    it("should not verify a tampered with checksum") {
      var checksum = checksumGenerator.generateChecksum(DATA)
      checksum += "ggg"

      assert(!checksumGenerator.validateChecksum(DATA, checksum))
    }

  }

}