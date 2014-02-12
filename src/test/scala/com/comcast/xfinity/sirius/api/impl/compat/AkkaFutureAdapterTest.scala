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
package com.comcast.xfinity.sirius.api.impl.compat

import com.comcast.xfinity.sirius.NiceTest
import org.scalatest.BeforeAndAfterAll
import scala.concurrent.{ExecutionContext, Future => AkkaFuture}
import akka.dispatch.ExecutionContexts._
import akka.actor.ActorSystem
import java.io.IOException
import java.util.concurrent.{ExecutionException, TimeUnit, TimeoutException}
import akka.dispatch.ExecutionContexts

class AkkaFutureAdapterTest extends NiceTest with BeforeAndAfterAll {

  implicit val as = ActorSystem("AkkaFutureAdapterTest")
  implicit val ec = ExecutionContexts.global()

  override def afterAll {
    as.shutdown()
    as.awaitTermination()
  }

  describe("AkkaFutureAdapter") {
    it("must throw an IllegalStateException when cancel is called") {
      intercept[IllegalStateException] {
        val akkaFuture = AkkaFuture { "foo" }
        new AkkaFutureAdapter[String](akkaFuture).cancel(true)
      }
    }

    it("must return the value expected on get") {
      assertResult("foo") {
        val akkaFuture = AkkaFuture { "foo" }
        new AkkaFutureAdapter[String](akkaFuture).get(2, TimeUnit.SECONDS)
      }
    }

    it("must throw a TimeoutException if it takes too long") {
      intercept[TimeoutException] {
        val akkaFuture = AkkaFuture { Thread.sleep(1000); "foo" }
        new AkkaFutureAdapter[String](akkaFuture).get(500, TimeUnit.MILLISECONDS)
      }
    }

    it("must propogate an exception as an ExecutionException") {
      intercept[ExecutionException] {
        val akkaFuture = AkkaFuture { throw new IOException("Boom") }
        new AkkaFutureAdapter[String](akkaFuture).get()
      }
    }
  }

}
