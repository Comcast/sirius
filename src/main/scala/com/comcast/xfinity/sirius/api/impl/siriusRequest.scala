package com.comcast.xfinity.sirius.api.impl

import java.util.Arrays

sealed trait SiriusRequest

case class Get(key: String) extends SiriusRequest

sealed trait NonIdempotentSiriusRequest extends SiriusRequest

case class Put(key: String, body: Array[Byte]) extends NonIdempotentSiriusRequest {
  override def equals(that: Any) = that match {
    case Put(`key`, thatBody) if Arrays.equals(body, thatBody) => true
    case _ => false
  }
}

case class Delete(key: String) extends NonIdempotentSiriusRequest