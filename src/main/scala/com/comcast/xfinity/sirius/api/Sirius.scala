package com.comcast.xfinity.sirius.api

import java.util.concurrent.Future

trait Sirius {
  
  /**
   * Enqueue a GET for processing
   */
  def enqueueGet(key: String): Future[SiriusResult]
  
  /**
   * Enqueue a PUT for processing
   */
  def enqueuePut(key: String, body: Array[Byte]): Future[SiriusResult]

  /**
   * Enqueue a DELETE for processing
   */
  def enqueueDelete(key: String): Future[SiriusResult]
}