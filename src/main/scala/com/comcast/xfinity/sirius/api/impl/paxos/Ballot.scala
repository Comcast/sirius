package com.comcast.xfinity.sirius.api.impl.paxos

object Ballot {
  val empty = Ballot(Int.MinValue, "")
}

case class Ballot(seq: Int, leaderId: String) extends Ordered[Ballot] {
  def compare(that: Ballot) = that match {
    case Ballot(thatSeq, _) if seq < thatSeq => -1
    case Ballot(thatSeq, _) if seq > thatSeq => 1
    case Ballot(_, thatLeaderId) if leaderId < thatLeaderId => -1
    case Ballot(_, thatLeaderId) if leaderId > thatLeaderId => 1
    case _ => 0
  }
}
