package com.comcast.xfinity.sirius.itest

import com.comcast.xfinity.sirius.api.RequestHandler
import collection.mutable.HashMap

class StringRequestHandler extends RequestHandler {

  val map: HashMap[String, Array[Byte]] = new HashMap[String, Array[Byte]]()

  /**
   * Handle a GET request
   */
  def handleGet(key: String): Array[Byte] = map.get(key).getOrElse(null)


  /**
   * Handle a PUT request
   */
  def handlePut(key: String, body: Array[Byte]): Array[Byte] = map.put(key, body).getOrElse(null)


  /**
   * Handle a DELETE request
   */
  def handleDelete(key: String): Array[Byte] = map.remove(key).getOrElse(null)

}