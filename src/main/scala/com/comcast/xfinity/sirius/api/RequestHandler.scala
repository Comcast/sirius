package com.comcast.xfinity.sirius.api

trait RequestHandler {

  /**
   * Handle a GET request
   */
  def handleGet(key: String): SiriusResult

  /**
   * Handle a PUT request
   */
  def handlePut(key: String, body: Array[Byte]): SiriusResult

  /**
   * Handle a DELETE request
   */
  def handleDelete(key: String): SiriusResult

}