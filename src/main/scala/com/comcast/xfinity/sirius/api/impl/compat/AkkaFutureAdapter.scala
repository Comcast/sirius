package com.comcast.xfinity.sirius.api.impl.compat

import akka.dispatch.{Await, Future => AkkaFuture}
import akka.util.duration._
import java.util.concurrent.{TimeoutException, ExecutionException, TimeUnit, Future}

class AkkaFutureAdapter[T](akkaFuture: AkkaFuture[T]) extends Future[T] {

  def cancel(mayInterrupt: Boolean): Boolean =
    throw new IllegalStateException("Not implemented")

  def isCancelled: Boolean = false // if not cancellable can't be cancelled

  def isDone: Boolean = akkaFuture.isCompleted

  def get: T =
    try {
      // there is no way in hell this can time out
      Await.result(akkaFuture, Long.MaxValue milliseconds)
    } catch {
      case e => throw new ExecutionException(e)
    }

  @throws(classOf[ExecutionException])
  @throws(classOf[TimeoutException])
  def get(l: Long, timeUnit: TimeUnit) =
    try {
      Await.result(akkaFuture, timeUnit.toMillis(l) milliseconds)
    } catch {
      case te: TimeoutException => throw te
      case e => throw new ExecutionException(e)
    }
}