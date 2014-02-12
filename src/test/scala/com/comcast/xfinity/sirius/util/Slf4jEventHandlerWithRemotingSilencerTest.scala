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

package com.comcast.xfinity.sirius.util

import com.comcast.xfinity.sirius.NiceTest
import akka.actor.ActorSystem
import org.scalatest.BeforeAndAfterAll
import akka.testkit.TestActorRef
import akka.event.Logging._
import java.io.FileNotFoundException

class Slf4jEventHandlerWithRemotingSilencerTest extends NiceTest with BeforeAndAfterAll {

  implicit val actorSystem = ActorSystem("Slf4jEventHandlerWithRemotingSilencerTest")

  override def afterAll() {
    actorSystem.shutdown()
  }

  it ("must drop all warn and error messages beginning with 'REMOTE: RemoteClient' or " +
      "'REMOTE: RemoteServer' and allow all others to pass through") {

    val handler = TestActorRef(new Slf4jEventHandlerWithRemotingSilencer {
      override def passThru(event: Any) {
        throw new FileNotFoundException("Because really, who's expecting that one?")
      }
    })

    handler receive Error(new Exception, "nowhere", getClass, "REMOTE: RemoteClientStuff...")
    handler receive Error(new Exception, "nowhere", getClass, "REMOTE: RemoteServerStuff...")

    handler receive Warning("nowhere", getClass, "REMOTE: RemoteClientStuff...")
    handler receive Warning("nowhere", getClass, "REMOTE: RemoteServerStuff...")

    intercept[FileNotFoundException] {
      handler receive Error(new Exception, "nowhere", getClass, "Not Remote stuff...")
    }

    intercept[FileNotFoundException] {
      handler receive Warning("nowhere", getClass, "Not remote stuff")
    }

  }
}
