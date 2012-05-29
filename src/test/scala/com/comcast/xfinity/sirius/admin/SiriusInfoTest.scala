package com.comcast.xfinity.sirius.admin
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.BeforeAndAfter
import org.scalatest.FunSpec

@RunWith(classOf[JUnitRunner])
class SiriusInfoTest extends FunSpec with BeforeAndAfter {

  var siriusInfo: SiriusInfo = _
  
  before {
    siriusInfo = new SiriusInfo(4242, "foobar")
  }
  
  describe("a SiriusInfo") {
    it("returns a name when getName is called") {
      assert("sirius-foobar:4242" == siriusInfo.getName())
    }
  }
}