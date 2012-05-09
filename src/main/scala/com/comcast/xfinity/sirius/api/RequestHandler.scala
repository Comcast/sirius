package com.comcast.xfinity.sirius.api

trait RequestHandler {
  
    def handle(method: RequestMethod, key: String, body: Array[Byte]): Array[Byte]
    
}