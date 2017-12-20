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

import java.util.concurrent.CompletableFuture

import scala.concurrent.Future
import scala.language.postfixOps
import scala.util.{Failure, Success}
import scala.concurrent.ExecutionContext.Implicits.global

/**
 * Class encapsulating a {@link akka.dispatch.Future} in a
 * {@link java.util.concurrent.Future}
 *
 * @param akkaFuture the {@see akka.dispatch.Future} to wrap
 */
class AkkaFutureAdapter[T](akkaFuture: Future[T]) extends CompletableFuture[T] {
  akkaFuture onComplete {
    case Success(result) => complete(result)
    case Failure(exception) => completeExceptionally(exception)
  }

  /**
    * Not implemented, you may not cancel an Akka Future
    */
  override def cancel(mayInterruptIfRunning: Boolean): Boolean =
    throw new IllegalStateException("Not implemented")
}
