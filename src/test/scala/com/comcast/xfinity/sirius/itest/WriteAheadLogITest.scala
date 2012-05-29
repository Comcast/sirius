package com.comcast.xfinity.sirius.itest

import com.comcast.xfinity.sirius.NiceTest
import com.comcast.xfinity.sirius.api.impl.SiriusImpl
import com.comcast.xfinity.sirius.api.RequestHandler
import collection.mutable.HashMap
import akka.actor.ActorSystem
import akka.dispatch.Await

import akka.util.duration._
import org.junit.rules.TemporaryFolder
import com.comcast.xfinity.sirius.writeaheadlog._

class WriteAheadLogITest extends NiceTest {

  var sirius: SiriusImpl = _

  var requestHandler: RequestHandler = _

  var actorSystem: ActorSystem = _
  val tempFolder = new TemporaryFolder()

  var logFilename: String = _


  before {
    tempFolder.create
    logFilename = tempFolder.newFile("sirius_wal.log").getAbsolutePath
    requestHandler = new StringRequestHandler();
    actorSystem = ActorSystem.create("Sirius")

    val logWriter: FileLogWriter = new FileLogWriter(logFilename, new WriteAheadLogSerDe())

    sirius = new SiriusImpl(requestHandler, actorSystem, logWriter)
  }

  after {
    actorSystem.shutdown
    tempFolder.delete

  }



  describe("a Sirius Write Ahead Log") {
    it("should have 1 entry after a PUT") {
      var logEntries = List[LogData]()

      // XXX: hack so that we can track the entries retrieved, in order
      val fn: LogData => Unit = (logData: LogData) => logEntries = logEntries ::: List(logData)

      Await.result(sirius.enqueuePut("1", "some body".getBytes), (5 seconds))

      val logReader: LogReader = new FileLogReader(logFilename, new WriteAheadLogSerDe())
      logReader.readEntries(fn)

      assert(1 === logEntries.size)
      assert("some body" === new String(logEntries(0).payload.get))
      assert("1" === logEntries(0).key)
    }
    it("should have 2 entries after 2 PUTs") {
      var logEntries = List[LogData]()

      // XXX: hack so that we can track the entries retrieved, in order
      val fn: LogData => Unit = (logData: LogData) => logEntries = logEntries ::: List(logData)


      Await.result(sirius.enqueuePut("1", "some body".getBytes), (5 seconds))
      Await.result(sirius.enqueuePut("2", "some other body".getBytes), (5 seconds))

      val logReader: LogReader = new FileLogReader(logFilename, new WriteAheadLogSerDe())
      logReader.readEntries(fn)



      assert(2 === logEntries.size)
      assert("some body" === new String(logEntries(0).payload.get))
      assert("1" === logEntries(0).key)
      assert("some other body" === new String(logEntries(1).payload.get))
      assert("2" === logEntries(1).key)
    }
  }


  private class StringRequestHandler extends RequestHandler {

    val map: HashMap[String, Array[Byte]] = new HashMap[String, Array[Byte]]()

    /**
     * Handle a GET request
     */
    def handleGet(key: String): Array[Byte] = map.get(key).getOrElse(null)


    /**
     * Handle a PUT request
     */
    def handlePut(key: String, body: Array[Byte]): Array[Byte] = map.put(key, body).getOrElse(null)


    /**
     * Handle a DELETE request
     */
    def handleDelete(key: String): Array[Byte] = map.remove(key).getOrElse(null)

  }

}