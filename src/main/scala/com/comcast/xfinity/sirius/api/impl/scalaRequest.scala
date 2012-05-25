package com.comcast.xfinity.sirius.api.impl

sealed trait SiriusRequest

case class Get(key: String) extends SiriusRequest

sealed trait NonIdempotentSiriusRequest extends SiriusRequest
case class Put(key: String, body: Array[Byte]) extends NonIdempotentSiriusRequest
case class Delete(key: String) extends NonIdempotentSiriusRequest

case class OrderedEvent(sequence :Long,  timestamp: Long, request: NonIdempotentSiriusRequest)