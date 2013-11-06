package com.comcast.xfinity.sirius.uberstore.common

import com.comcast.xfinity.sirius.NiceTest

class Fnv1aChecksummerTest extends NiceTest {

  val underTest = new Object with Fnv1aChecksummer

  it ("must just be the base offset for the empty string") {
    assert(-3750763034362895579L === underTest.checksum(new Array[Byte](0)))
  }

  it ("must match the reference impl") {
    // from:
    //   http://trac.tools.ietf.org/wg/tls/draft-ietf-tls-cached-info/draft-ietf-tls-cached-info-06-from-05.diff.txt
    val referenceBytes = "http://en.wikipedia.org/wiki/Fowler_Noll_Vo_hash".getBytes
    assert(-2758076559093427003L === underTest.checksum(referenceBytes))
  }
}