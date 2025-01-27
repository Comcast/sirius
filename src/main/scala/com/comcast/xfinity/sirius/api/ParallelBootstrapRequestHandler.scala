package com.comcast.xfinity.sirius.api

object ParallelBootstrapRequestHandler {
    def apply(requestHandler: RequestHandler): ParallelBootstrapRequestHandler =
        new ParallelBootstrapRequestHandler(requestHandler)
}

class ParallelBootstrapRequestHandler(val requestHandler: RequestHandler) extends AbstractParallelBootstrapRequestHandler[String, Array[Byte]] {
    override protected def enabled(): Boolean = true
    override protected def createKey(key: String): String = key
    override protected def deserialize(body: Array[Byte]): Array[Byte] = body
    override def handleGetImpl(key: String): SiriusResult = requestHandler.handleGet(key)
    override def handlePutImpl(key: String, body: Array[Byte]): SiriusResult = requestHandler.handlePut(key, body)
    override def handleDeleteImpl(key: String): SiriusResult = requestHandler.handleDelete(key)
    override def handlePutImpl(sequence: Long, key: String, body: Array[Byte]): SiriusResult = requestHandler.handlePut(sequence, key, body)
    override def handleDeleteImpl(sequence: Long, key: String): SiriusResult = requestHandler.handleDelete(sequence, key)

    override def onBootstrapStartingImpl(): Unit = requestHandler.onBootstrapStarting()
    override def onBootstrapCompletedImpl(): Unit = requestHandler.onBootstrapComplete()
}
