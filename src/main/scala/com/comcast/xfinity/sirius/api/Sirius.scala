package com.comcast.xfinity.sirius.api
import akka.dispatch.Future

trait Sirius {
    def enqueue(method:RequestMethod, key: String, body: Array[Byte]): Future[Array[Byte]]

    def enqueuePut(key: String, body: Array[Byte]): Future[Array[Byte]]

    def enqueueGet(key: String): Future[Array[Byte]]
}