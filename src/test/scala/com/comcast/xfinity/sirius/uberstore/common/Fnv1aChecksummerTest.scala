/*
 *  Copyright 2012-2014 Comcast Cable Communications Management, LLC
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

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
