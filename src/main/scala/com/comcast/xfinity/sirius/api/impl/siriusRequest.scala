package com.comcast.xfinity.sirius.api.impl

import java.util.Arrays

sealed trait SiriusRequest

case class Get(key: String) extends SiriusRequest

sealed trait NonCommutativeSiriusRequest extends SiriusRequest

// XXX: hashCode may not be reliable due to Array[Byte] from Java sucking
case class Put(key: String, body: Array[Byte]) extends NonCommutativeSiriusRequest {
  override def equals(that: Any) = that match {
    case Put(`key`, thatBody) if Arrays.equals(body, thatBody) => true
    case _ => false
  }
}

case class Delete(key: String) extends NonCommutativeSiriusRequest