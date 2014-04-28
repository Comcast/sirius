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
package com.comcast.xfinity.sirius.api.impl

import com.comcast.xfinity.sirius.{SiriusFactory => NewFactory}
import com.comcast.xfinity.sirius.api.RequestHandler
import com.comcast.xfinity.sirius.api.SiriusConfiguration
import com.comcast.xfinity.sirius.writeaheadlog.SiriusLog

/**
 * Compatibility wrapper for factory method now in top-level sirius package.
 * Since the three-arg testing method was intended to only be used for internal
 * testing it has not been wrapped.
 */
object SiriusFactory {

  /**
   * Deprecated - see [[com.comcast.xfinity.sirius.SiriusFactory]]
   */
  @deprecated("see top-level SiriusFactory object", "1.2.0")
  def createInstance(requestHandler: RequestHandler,
                     siriusConfig: SiriusConfiguration): SiriusImpl = {
    val res = NewFactory.createInstance(requestHandler, siriusConfig)
    res.asInstanceOf[SiriusImpl]
  }
}
