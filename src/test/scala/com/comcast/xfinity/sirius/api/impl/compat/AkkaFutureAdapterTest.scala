package com.comcast.xfinity.sirius.api.impl.compat

import com.comcast.xfinity.sirius.NiceTest
import org.scalatest.BeforeAndAfterAll
import akka.dispatch.{ExecutionContext, Future => AkkaFuture}
import akka.actor.ActorSystem
import java.io.IOException
import java.util.concurrent.{ExecutionException, TimeUnit, TimeoutException}

class AkkaFutureAdapterTest extends NiceTest with BeforeAndAfterAll {

  implicit val as = ActorSystem("AkkaFutureAdapterTest")
  implicit val ec = ExecutionContext.defaultExecutionContext

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
      expect("foo") {
        val akkaFuture = AkkaFuture { "foo" }
        new AkkaFutureAdapter[String](akkaFuture).get()
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