package com.comcast.xfinity.sirius.api.impl

sealed trait SiriusRequest

case class Put(key: String, body: Array[Byte]) extends SiriusRequest
case class Get(key: String) extends SiriusRequest
case class Delete(key: String) extends SiriusRequest


case class OrderedEvent(seqeuence :Long,  timestamp: Long, request: SiriusRequest)