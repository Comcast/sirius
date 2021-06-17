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
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

/**
 * Class encapsulating a {@link scala.concurrent.Future} in a
 * {@link java.util.concurrent.CompletableFuture}
 *
 * @param akkaFuture the {@see scala.concurrent.Future} to wrap
 */
class AkkaFutureAdapter[T](akkaFuture: Future[T])(implicit ec: ExecutionContext) extends CompletableFuture[T] {
  akkaFuture.onComplete {
    case Success(value) => complete(value)
    case Failure(exception) => completeExceptionally(exception)
  }
}
