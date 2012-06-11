package com.comcast.xfinity.sirius.info

import com.comcast.xfinity.sirius.NiceTest

class SiriusInfoTest extends NiceTest {

  var siriusInfo: SiriusInfo = _

  before {
    siriusInfo = new SiriusInfo(4242, "foobar")
  }

  describe("a SiriusInfo") {
    it("returns a name when getName is called") {
      assert("sirius-foobar:4242" == siriusInfo.getName())
    }
    it("should return getname when toString is called") {
      assert(siriusInfo.getName() === siriusInfo.toString())
    }
  }
}