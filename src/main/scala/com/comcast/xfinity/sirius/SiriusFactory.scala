/*
 *  Copyright 2012-2013 Comcast Cable Communications Management, LLC
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
package com.comcast.xfinity.sirius

import com.comcast.xfinity.sirius.api._

/**
 * Factory for [[com.comcast.xfinity.sirius.api.Sirius]] instances
 */
object SiriusFactory {

  /**
   * SiriusImpl factory method, takes parameters to construct a
   * SiriusImplementation and the dependent ActorSystem and return the
   * created instance.  Calling shutdown on the produced SiriusImpl
   * will also shutdown the dependent ActorSystem.
   *
   * @param requestHandler the RequestHandler containing callbacks for
   *  manipulating the system's state
   * @param siriusConfig a SiriusConfiguration containing configuration
   *  info needed for this node.
   * @see SiriusConfiguration for info on needed config.
   *
   * @return A Sirius(Impl) constructed using the parameters
   */
  def createInstance(requestHandler: RequestHandler,
                     siriusConfig: SiriusConfiguration): Sirius = {
    impl.SiriusFactory.createInstance(requestHandler, siriusConfig)
  }
}
