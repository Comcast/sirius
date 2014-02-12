/**
 * Copyright 2014 Comcast Cable Communications Management, LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.comcast.xfinity.sirius.api.impl

import com.comcast.xfinity.sirius.api.Sirius

/**
 * These are additional methods supported by an expanded Sirius interface.
 */
trait Sirius1Dot2Extensions {
  /**
   * Register a callback to be invoked once the Sirius subsystem has been
   * initialized (i.e. log replay has completed). This may be called
   * multiple times to install multiple init hook callbacks; each will be
   * called once upon initialization. If Sirius has already been
   * initialized, the callback will be invoked right away.
   *
   * @param initHook callback to run
   */
  def onInitialized(initHook: Runnable): Unit
}

trait Sirius1Dot2 extends Sirius with Sirius1Dot2Extensions
