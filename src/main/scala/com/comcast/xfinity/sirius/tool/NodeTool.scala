/*
 *  Copyright 2012-2014 Comcast Cable Communications Management, LLC
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.comcast.xfinity.sirius.tool

import helper.ActorSystemHelper
import akka.util.Timeout
import scala.concurrent.duration._
import akka.pattern.ask
import scala.concurrent.Await
import akka.actor.ActorSelection
import com.comcast.xfinity.sirius.api.impl.status.StatusWorker._
import com.comcast.xfinity.sirius.api.impl.status.NodeStats.FullNodeStatus
import com.comcast.xfinity.sirius.util.SiriusShortNameParser
import scala.language.postfixOps
import com.comcast.xfinity.sirius.api.impl.state.SiriusPersistenceActor.{NewLogSubrange => LogSubrange}
import com.comcast.xfinity.sirius.api.impl.state.SiriusPersistenceActor.{CompleteSubrange, PartialSubrange, EmptySubrange, GetNextLogSeq, GetLogSubrange}

/**
 * Object meant to be invoked as a main class from the terminal.  Provides some
 * simple node query operations.
 *
 * See usage for usage.
 */
object NodeTool {

  // TODO: make configurable via system property
  implicit val timeout: Timeout = 5 seconds

  def main(args: Array[String]) {
    try {
      doMain(args)
    } finally {
      ActorSystemHelper.shutDownActorSystem()
    }
  }

  // put main here to avoid excessive nesting
  private def doMain(args: Array[String]) {
    args match {
      case Array("next-seq", nodeId) =>
        val ref = getNodeRef(nodeId)
        println(getNextSeq(ref))

      case Array("log-range", begin, end, nodeId) =>
        val ref = getNodeRef(nodeId)
        getLogRange(ref, begin.toLong, end.toLong) match {
          case CompleteSubrange(_, _, events) => events.foreach(println)
          case PartialSubrange(_, _, events) => events.foreach(println)
          case EmptySubrange =>
        }

      case Array("log-tail", nodeId) =>
        val ref = getNodeRef(nodeId)
        val lastSeq = getNextSeq(ref) - 1
        getLogRange(ref, lastSeq - 20, lastSeq) match {
          case CompleteSubrange(_, _, events) => events.foreach(println)
          case PartialSubrange(_, _, events) => events.foreach(println)
          case EmptySubrange =>
        }

      case Array("status", nodeId) =>
        val ref = getNodeRef(nodeId)
        println(getNodeStatus(ref))

      // XXX: the following is a short term fix, if we want to be able to force election
      //      we should talk about putting the LeaderGone message elsewhere
      case Array("force-seek-leadership", nodeId) =>
        val leaderAddr = getNodeAddressString(nodeId) + "/paxos/leader"
        val leaderRef = ActorSystemHelper.getActorSystem().actorSelection(leaderAddr)
        // XXX: using the full path here because this is dirty, and that should be known!
        leaderRef ! com.comcast.xfinity.sirius.api.impl.paxos.LeaderWatcher.LeaderGone

      case _ =>
        printUsage()
        System.exit(1)
    }
  }

  /**
   * Get the next expected sequence number from sirius node ref
   *
   * @param ref ActorRef for node to query
   *
   * @return Long value of the next expected sequence number
   */
  private def getNextSeq(ref: ActorSelection) = {
    val nextSeqFuture = ask(ref, GetNextLogSeq).mapTo[Long]
    Await.result(nextSeqFuture, timeout.duration)
  }

  /**
   * Get the subrange specified by begin and end from sirius node ref
   *
   * @param ref ActorRef for node to query
   * @param begin beginning of range
   * @param end end of range
   *
   * @return a LogSubrange containing as many events as were retrievable
   */
  private def getLogRange(ref: ActorSelection, begin: Long, end: Long): LogSubrange = {
    val rangeFuture = ask(ref, GetLogSubrange(begin, end)).mapTo[LogSubrange]
    Await.result(rangeFuture, timeout.duration)
  }

  /**
   * Get a nodes status
   *
   * @param ref ActorRef for node to query
   *
   * @return FullNodeStatus with lots of metadata about that node
   */
  private def getNodeStatus(ref: ActorSelection): FullNodeStatus = {
    val statusFuture = ask(ref, GetStatus).mapTo[FullNodeStatus]
    Await.result(statusFuture, timeout.duration)
  }

  private def getNodeAddressString(shortAddrStr: String): String =
    SiriusShortNameParser.parse(shortAddrStr) match {
      case Some(addrStr) => addrStr
      case None => throw new IllegalArgumentException(shortAddrStr + " does not appear to be a vaild " +
          "Akka address or Sirius node short name")
    }

  private def getNodeRef(shortAddrStr: String): ActorSelection = {
    val fullAddress = getNodeAddressString(shortAddrStr)
    ActorSystemHelper.getActorSystem().actorSelection(fullAddress)
  }


  private def printUsage() {
    Console.err.println("Usage:")
    Console.err.println("   next-seq <nodeId>")
    Console.err.println("       Get the next sequence number of node identified by nodeId")
    Console.err.println()
    Console.err.println("   log-range <begin> <end> <nodeId>")
    Console.err.println("       Get log subrange between begin and end inclusive from nodeId")
    Console.err.println()
    Console.err.println("   log-tail <nodeId>")
    Console.err.println("       Print the last 20 events in nodeId's log")
    Console.err.println()
    Console.err.println("   status <nodeId>")
    Console.err.println("       Get general status information for nodeId")
    Console.err.println()
    Console.err.println("   force-seek-leadership <nodeId>")
    Console.err.println("       Force nodeId to seek leadership. This is a hack around ")
    Console.err.println("       \"phantom ballots from the future\", but it works, for now")
  }

}
