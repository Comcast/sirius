package com.comcast.xfinity.sirius.uberstore.seqindex

trait SeqIndex {
  def getOffsetFor(seq: Long): Option[Long]
  def put(seq: Long, offset: Long)
  def getMaxSeq: Long
}