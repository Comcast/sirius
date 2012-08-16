import com.comcast.xfinity.sirius.api._
import com.comcast.xfinity.sirius.api.impl._
import com.comcast.xfinity.sirius.api.impl.persistence._
import com.comcast.xfinity.sirius.writeaheadlog._

import scalax.io.CloseableIterator

println()
println("Most of the Sirius classes have been imported, nonetheless we may have missed some...")
println("As a convenience, some akka classes have been imported as well")
println()

def sirius(clusterConfigPath: String, usePaxos:Boolean = false, host:String="localhost", port:Int = 2552,  walLoc:String= "./wal.log"): Sirius = {
  val dumbLog = new SiriusLog() {


    override def writeEntry(entry: OrderedEvent) {

    }

    override def foldLeft[T](acc0: T)(foldFun: (T, OrderedEvent) => T): T = acc0

    override def createIterator(logRange: LogRange): CloseableIterator[OrderedEvent] =
      CloseableIterator(Iterator[OrderedEvent]())

    override def getNextSeq = 1L

  }

  val dumbHandler = new RequestHandler() {

    /**
     * Handle a GET request
     */
    def handleGet(key: String): SiriusResult = {
         SiriusResult.none
    }


    /**
     * Handle a PUT request
     */
    def handlePut(key: String, body: Array[Byte]): SiriusResult = {
        SiriusResult.none
    }

    /**
     * Handle a DELETE request
     */
    def handleDelete(key: String): SiriusResult = {
        SiriusResult.none
    }
  }

  SiriusImpl.createSirius(dumbHandler, SiriusConfiguration(host, port, clusterConfigPath, usePaxos, walLoc), dumbLog)
}


