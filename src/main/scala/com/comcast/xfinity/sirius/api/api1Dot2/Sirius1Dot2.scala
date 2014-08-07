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
package com.comcast.xfinity.sirius.api.api1Dot2

import com.comcast.xfinity.sirius.api.{Sirius => Sirius1Dot1}

/**
 * Methods added to the abstract Sirius interface in the 1.2.x series release(s).
 */
trait Sirius1Dot2Extensions {

  /**
   * Terminate this instance, including shutting down all internal running Actors.
   */
  def shutdown(): Unit
}

/**
 * Enhanced abstract Sirius interface made available in the 1.2.x series release(s).
 */
trait Sirius1Dot2 extends Sirius1Dot1 with Sirius1Dot2Extensions

