package com.comcast.xfinity.sirius.api.impl.persistence
import akka.actor.{ActorRef, FSM, Actor}
import org.slf4j.LoggerFactory
import com.comcast.xfinity.sirius.writeaheadlog.LogLinesSource
import scalax.io.CloseableIterator

// received messages
case class Start(ref: ActorRef, input: LogLinesSource, chunkSize: Int)
case object StartSending
case class Received(seqRecd: Int)
case class Processed(seqRecd: Int)
case object DoneAck

// sent messages
case class LogChunk(seqSent: Int, chunk: Seq[String])
case object DoneMsg

// FSM States
sealed trait LSState
case object Uninitialized extends LSState
case object Waiting extends LSState
case object Sending extends LSState
case object Done extends LSState

// FSM Data Types
sealed trait LSData
case object Null extends LSData
case class SendingData(target: ActorRef, lines: CloseableIterator[String], seqNum: Int, chunkSize: Int) extends LSData

/**
 * FSM that spins up and sends logs in chunks to another actor, usually a LogReceivingActor
 */
class LogSendingActor extends Actor with FSM[LSState, LSData] {
  private val logger = LoggerFactory.getLogger(classOf[LogSendingActor])

  startWith(Uninitialized, Null)

  when(Uninitialized) {
    case Event(Start(target, input, chunkSize), Null) =>
      goto(Waiting) using SendingData(target, input.createLinesIterator(), 0, chunkSize)
  }

  when(Waiting) {
    case Event(StartSending, data: SendingData) =>
      // send first chunk of data
      goto(Sending) using data.copy(seqNum = data.seqNum + 1)

    // if we got the seqRecd we expected AND there's no more to send: we're done
    case Event(Processed(seqRecd: Int), data: SendingData) if seqRecd == data.seqNum && !data.lines.hasNext =>
      data.lines.close()
      goto(Done) using data

    // otherwise, if we got the seqRecd we expected
    case Event(Processed(seqRecd: Int), data: SendingData) if seqRecd == data.seqNum =>
      goto(Sending) using data.copy(seqNum = data.seqNum + 1)

    // got a seqRecd we did NOT expect
    case Event(Processed(seqRecd: Int), data: SendingData) =>
      val reason = "In Waiting, got <Received, SendingData> but Sequence Number is wrong! Expected:"+data.seqNum+" Received:"+seqRecd
      logger.warn(reason)
      stop(FSM.Failure(reason))
  }

  when(Sending) {
    case Event(recv: Received, data: SendingData) if recv.seqRecd == data.seqNum =>
      goto(Waiting) using data
  }

  when(Done) {
    case Event(DoneAck, data: SendingData) =>
      // stop happily
      stop(FSM.Normal)
  }

  def gatherData(lines: CloseableIterator[String], chunkSize: Int): Seq[String] = {
    def gatherDataAux(lines: CloseableIterator[String], more: Int, accum: Seq[String]): Seq[String] = {
      if (more == 0 || !lines.hasNext)
        accum
      else
        gatherDataAux(lines, more - 1, accum :+ lines.next())
    }
    gatherDataAux(lines, chunkSize, Vector.empty[String])
  }

  onTransition {
    case Uninitialized -> Waiting => {
      stateData match {
        case Null =>
          // kick off log sending
          self ! StartSending
        case _ =>
          val reason = "On Uninitialized -> Waiting transition, Unhandled <stateName, stateData>: <"+stateName+", "+stateData+">"
          logger.warn(reason)
          stop(FSM.Failure(reason))
      }
    }
    case Waiting -> Sending => {
      nextStateData match {
        case SendingData(target, lines, seqNum, chunkSize) =>
          // send next chunk
          val logChunk: LogChunk = new LogChunk(seqNum, gatherData(lines, chunkSize))
          target ! logChunk
        case _ =>
          val reason = "On Waiting -> Sending transition, Unhandled <stateName, stateData>: <"+stateName+", "+stateData+">"
          logger.warn(reason)
          stop(FSM.Failure(reason))
      }
    }
    case Waiting -> Done => {
      stateData match {
        case SendingData(target, lines, seqNum, chunkSize) =>
          target ! DoneMsg
        case _ =>
          val reason = "On Waiting -> Done transition, Unhandled <stateName, stateData>: <"+stateName+", "+stateData+">"
          logger.debug(reason)
          stop(FSM.Failure(reason))
      }
    }
  }

  whenUnhandled {
    case Event(event, data) =>
      data match {
        case SendingData(target, lines, seqNum, chunkSize) =>
          // something bad happened; close our data iterator
          lines.close()
        case _ =>
      }
      val reason = "Received unhandled request " + event + " in state " + stateName + "/" + data
      logger.debug(reason)
      context.parent ! TransferFailed(reason)
      stop(FSM.Failure(reason))
  }

  initialize
}
