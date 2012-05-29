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

  var actorSystem: ActorSystem = _

  val tempFolder = new TemporaryFolder()
  var logFilename: String = _

  var logReader: LogReader = _

  before {
    tempFolder.create
    logFilename = tempFolder.newFile("sirius_wal.log").getAbsolutePath

    actorSystem = ActorSystem.create("Sirius")

    val logWriter: FileLogWriter = new FileLogWriter(logFilename, new WriteAheadLogSerDe())

    sirius = new SiriusImpl(new StringRequestHandler(), actorSystem, logWriter)

    logReader = new FileLogReader(logFilename, new WriteAheadLogSerDe())
  }

  after {
    actorSystem.shutdown
    tempFolder.delete

  }



  describe("a Sirius Write Ahead Log") {
    it("should have 1 entry after a PUT") {
      Await.result(sirius.enqueuePut("1", "some body".getBytes), (5 seconds))
      
      val logEntries = logReader.foldLeft(List[LogData]())((a: List[LogData], c) => a ::: List(c))

      assert(1 === logEntries.size)
      assert("some body" === new String(logEntries(0).payload.get))
      assert("1" === logEntries(0).key)
    }
    it("should have 2 entries after 2 PUTs") {
      Await.result(sirius.enqueuePut("1", "some body".getBytes), (5 seconds))
      Await.result(sirius.enqueuePut("2", "some other body".getBytes), (5 seconds))
      
      val logEntries = logReader.foldLeft(List[LogData]())((a: List[LogData], c) => a ::: List(c))

      assert(2 === logEntries.size)
      assert("some body" === new String(logEntries(0).payload.get))
      assert("1" === logEntries(0).key)
      assert("some other body" === new String(logEntries(1).payload.get))
      assert("2" === logEntries(1).key)
    }
    it("should have a PUT and a DELETE entry after a PUT and a DELETE") {
      Await.result(sirius.enqueuePut("1", "some body".getBytes), (5 seconds))
      Await.result(sirius.enqueueDelete("1"), (5 seconds))
      
      val logEntries = logReader.foldLeft(List[LogData]())((a: List[LogData], c) => a ::: List(c))
      
      assert(2 === logEntries.size)

      assert("PUT" === logEntries(0).actionType)
      assert("some body" === new String(logEntries(0).payload.get))
      assert("1" === logEntries(0).key)
      
      assert("DELETE" === logEntries(1).actionType)
      assert("" === new String(logEntries(1).payload.get))
      assert("1" === logEntries(1).key)
    }
    
    it("should have 2 PUT entries after 2 PUTs and a GET") {
      Await.result(sirius.enqueuePut("1", "some body".getBytes), (5 seconds))
      Await.result(sirius.enqueuePut("2", "some other body".getBytes), (5 seconds))
      Await.result(sirius.enqueueGet("1"), (5 seconds))

      val logEntries = logReader.foldLeft(List[LogData]())((a: List[LogData], c) => a ::: List(c))

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