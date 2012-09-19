package com.comcast.xfinity.sirius.tool

import helper.ActorSystemHelper
import akka.util.Timeout
import akka.util.duration._
import akka.pattern.ask
import akka.dispatch.Await
import com.comcast.xfinity.sirius.api.impl.state.SiriusPersistenceActor._
import akka.actor.ActorRef

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
        val ref = ActorSystemHelper.getActorSystem().actorFor(nodeId)
        println(getNextSeq(ref))

      case Array("log-range", begin, end, nodeId) =>
        val ref = ActorSystemHelper.getActorSystem().actorFor(nodeId)
        getLogRange(ref, begin.toLong, end.toLong).events.foreach(println)

      case Array("log-tail", nodeId) =>
        val ref = ActorSystemHelper.getActorSystem().actorFor(nodeId)
        val lastSeq = getNextSeq(ref) - 1
        getLogRange(ref, lastSeq - 20, lastSeq).events.foreach(println)

      case _ =>
        printUsage()
        System.exit(1)
    }
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
  private def getLogRange(ref: ActorRef, begin: Long, end: Long): LogSubrange = {
    val rangeFuture = ask(ref, GetLogSubrange(begin, end)).mapTo[LogSubrange]
    Await.result(rangeFuture, timeout.duration)
  }

  /**
   * Get the next expected sequence number from sirius node ref
   *
   * @param ref ActorRef for node to query
   *
   * @return Long value of the next expected sequence number
   */
  private def getNextSeq(ref: ActorRef) = {
    val nextSeqFuture = ask(ref, GetNextLogSeq).mapTo[Long]
    Await.result(nextSeqFuture, timeout.duration)
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
  }

}