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
package com.comcast.xfinity.sirius.api.impl.membership

import com.comcast.xfinity.sirius.NiceTest
import org.mockito.Mockito._

class BackwardsCompatibleClusterConfigTest extends NiceTest {
  describe("members") {
    it("Must convert the backend members response if there exist any akka:// paths") {
      val mockBackend = mock[ClusterConfig]
      val underTest = new BackwardsCompatibleClusterConfig(mockBackend)
      val backendList = List("akka://one", "akka://two", "akka.tcp://three")
      val expected = List("akka.tcp://one", "akka.tcp://two", "akka.tcp://three")

      doReturn(backendList).when(mockBackend).members

      assert(expected === underTest.members)
    }

    it("Must ONLY convert akka:// strings if they are a prefix") {
      val mockBackend = mock[ClusterConfig]
      val underTest = new BackwardsCompatibleClusterConfig(mockBackend)
      val backendList = List("akka.tcp://one/terrible/actor/path/in/akka://")

      doReturn(backendList).when(mockBackend).members

      assert(backendList === underTest.members)
    }

    it("Must change nothing if there do not exist any akka:// paths") {
      val mockBackend = mock[ClusterConfig]
      val underTest = new BackwardsCompatibleClusterConfig(mockBackend)
      val backendList = List("akka.tcp://one", "akka.tcp://two")

      doReturn(backendList).when(mockBackend).members

      assert(backendList === underTest.members)
    }
  }

}
