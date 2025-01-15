package com.comcast.xfinity.sirius.uberstore.common

import com.comcast.xfinity.sirius.NiceTest

class SkipValidationChecksummerTest extends NiceTest {
    val underTest = new Object with Fnv1aChecksummer with SkipValidationChecksummer

    it ("must match the reference impl") {
        // from:
        //   http://trac.tools.ietf.org/wg/tls/draft-ietf-tls-cached-info/draft-ietf-tls-cached-info-06-from-05.diff.txt
        val referenceBytes = "http://en.wikipedia.org/wiki/Fowler_Noll_Vo_hash".getBytes
        assert(-2758076559093427003L === underTest.checksum(referenceBytes))
    }

    it ("skips validating the checksum") {
        val referenceBytes = "http://en.wikipedia.org/wiki/Fowler_Noll_Vo_hash".getBytes
        assert(true === underTest.validate(referenceBytes, 0L))
    }
}
