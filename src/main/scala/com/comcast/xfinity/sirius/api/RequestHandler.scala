package com.comcast.xfinity.sirius.api

trait RequestHandler {

    def handle(method: RequestMethod, key: String, body: Array[Byte]): Array[Byte]

  /**
   * Handle a GET request
   */
  def handleGet(key: String): Array[Byte]

  /**
   * Handle a PUT request
   */
  def handlePut(key: String, body: Array[Byte]): Array[Byte]

  /**
   * Handle a DELETE request
   */
  def handleDelete(key: String): Array[Byte]

}