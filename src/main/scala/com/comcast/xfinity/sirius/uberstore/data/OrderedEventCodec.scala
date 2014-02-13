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
package com.comcast.xfinity.sirius.uberstore.data

import com.comcast.xfinity.sirius.api.impl.OrderedEvent

/**
 * Trait supplying functions for converting OrderedEvents to and
 * from Array[Byte]s
 */
trait OrderedEventCodec {
  /**
   * Serialize event to Array[Byte]
   *
   * @param event the OrderedEvent to serialize
   *
   * @return the serialized OrderedEvent
   */
  def serialize(event: OrderedEvent): Array[Byte]

  /**
   * Read an OrderedEvent from an Array[Byte]
   *
   * If you give it a bad event, bad things will happen, don't be that guy
   *
   * @param bytes the Array[Byte] to decode
   *
   * @return the OrderedEvent decoded from bytes
   */
  def deserialize(bytes: Array[Byte]): OrderedEvent
}
