package com.comcast.xfinity.sirius.api

abstract class AbstractRequestHandler extends RequestHandler {
    def handle(method: RequestMethod, key: String, body: Array[Byte]): Array[Byte] = method match {
      case RequestMethod.PUT => doPut(key, body)
      case RequestMethod.GET => doGet(key, body)
      case RequestMethod.DELETE => doDelete(key, body)
    }
    
    def doPut(key: String, body: Array[Byte]): Array[Byte]
    def doGet(key: String, body: Array[Byte]): Array[Byte]
    def doDelete(key: String, body: Array[Byte]): Array[Byte]
}
