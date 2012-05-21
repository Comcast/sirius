package com.comcast.xfinity.sirius.api
import akka.dispatch.Future

trait Sirius {
  
  /**
   * Enqueue a GET for processing
   */
  def enqueueGet(key: String): Future[Array[Byte]]
  
  /**
   * Enqueue a PUT for processing
   */
  def enqueuePut(key: String, body: Array[Byte]): Future[Array[Byte]]

  /**
   * Enqueue a DELETE for processing
   */
  def enqueueDelete(key: String): Future[Array[Byte]]
}