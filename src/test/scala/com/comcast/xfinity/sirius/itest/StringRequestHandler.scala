package com.comcast.xfinity.sirius.itest

import com.comcast.xfinity.sirius.api.RequestHandler
import collection.mutable.HashMap
import com.comcast.xfinity.sirius.api.SiriusResult

class StringRequestHandler extends RequestHandler {

  var cmdsHandledCnt = 0;
  val map: HashMap[String, Array[Byte]] = new HashMap[String, Array[Byte]]()

  /**
   * Handle a GET request
   */
  def handleGet(key: String): SiriusResult = {
    cmdsHandledCnt += 1;
    map.get(key) match {
      case Some(v) => SiriusResult.some(v)
      case None => SiriusResult.none()
    }
  }


  /**
   * Handle a PUT request
   */
  def handlePut(key: String, body: Array[Byte]): SiriusResult = {
    cmdsHandledCnt += 1;
    map.put(key, body) match {
      case Some(v) => SiriusResult.some(v)
      case None => SiriusResult.none()
    }
  }

  /**
   * Handle a DELETE request
   */
  def handleDelete(key: String): SiriusResult = {
    cmdsHandledCnt += 1;
    map.remove(key) match {
      case Some(v) => SiriusResult.some(v)
      case None => SiriusResult.none()
    }
  }
}