/*
 *  Copyright 2012-2019 Comcast Cable Communications Management, LLC
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

package com.comcast.xfinity.sirius.uberstore.data

import com.comcast.xfinity.sirius.NiceTest
import com.comcast.xfinity.sirius.api.SiriusConfiguration

class UberDataFileHandleFactoryTest extends NiceTest {

  describe("apply") {
    it("returns RandomAccessFileHandleFactory when LOG_USE_READ_BUFFER is false") {
      val siriusConfig = new SiriusConfiguration
      siriusConfig.setProp(SiriusConfiguration.LOG_USE_READ_BUFFER, false)

      val fileHandleFactory = UberDataFileHandleFactory(siriusConfig)
      assert(fileHandleFactory == RandomAccessFileHandleFactory)
    }
    it("returns RandomAccessFileHandleFactory when LOG_USE_READ_BUFFER is not set") {
      val siriusConfig = new SiriusConfiguration

      val fileHandleFactory = UberDataFileHandleFactory(siriusConfig)
      assert(fileHandleFactory == RandomAccessFileHandleFactory)
    }
    it("returns RandomAccessFileHandleFactory when LOG_USE_READ_BUFFER is true and LOG_READ_BUFFER_SIZE_BYTES is zero") {
      val siriusConfig = new SiriusConfiguration
      siriusConfig.setProp(SiriusConfiguration.LOG_USE_READ_BUFFER, true)
      siriusConfig.setProp(SiriusConfiguration.LOG_READ_BUFFER_SIZE_BYTES, 0)

      val fileHandleFactory = UberDataFileHandleFactory(siriusConfig)
      assert(fileHandleFactory == RandomAccessFileHandleFactory)
    }
    it("returns BufferedReadFileHandleFactory when LOG_USE_READ_BUFFER is true and LOG_READ_BUFFER_SIZE_BYTES is greater than zero") {
      val siriusConfig = new SiriusConfiguration
      siriusConfig.setProp(SiriusConfiguration.LOG_USE_READ_BUFFER, true)
      siriusConfig.setProp(SiriusConfiguration.LOG_READ_BUFFER_SIZE_BYTES, 1)

      val fileHandleFactory = UberDataFileHandleFactory(siriusConfig)
      assert(fileHandleFactory.isInstanceOf[BufferedReadFileHandleFactory])
    }
    it("returns BufferedReadFileHandleFactory when LOG_USE_READ_BUFFER is true and LOG_READ_BUFFER_SIZE_BYTES is not set") {
      val siriusConfig = new SiriusConfiguration
      siriusConfig.setProp(SiriusConfiguration.LOG_USE_READ_BUFFER, true)

      val fileHandleFactory = UberDataFileHandleFactory(siriusConfig)
      assert(fileHandleFactory.isInstanceOf[BufferedReadFileHandleFactory])
    }
  }
}
