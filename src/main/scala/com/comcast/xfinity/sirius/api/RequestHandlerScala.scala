package com.comcast.xfinity.sirius.api

trait RequestHandlerScala {
  
    def handle(method: RequestMethod, key: String, body: Array[Byte]): Array[Byte]
    
}