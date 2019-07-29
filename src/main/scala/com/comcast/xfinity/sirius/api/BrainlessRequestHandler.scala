package com.comcast.xfinity.sirius.api

/**
  * Special instance of [[RequestHandler]] that indicates to Sirius to not bootstrap the in-memory brain
  */
case object BrainlessRequestHandler extends RequestHandler {
  /**
    * Handle a GET request
    *
    * @param key String identifying the search query
    * @return a SiriusResult wrapping the result of the query
    */
  override def handleGet(key: String): SiriusResult = SiriusResult.none()

  /**
    * Handle a PUT request
    *
    * @param key  unique identifier for the item to which the
    *             operation is being applied
    * @param body data passed in along with this request used
    *             for modifying the state at key
    * @return a SiriusResult wrapping the result of the operation.
    *         This should almost always be SiriusResult.none().
    *         In the future the API may be modified to return void.
    */
  override def handlePut(key: String, body: Array[Byte]): SiriusResult = SiriusResult.none()

  /**
    * Handle a DELETE request
    *
    * @param key unique identifier for the item to which the
    *            operation is being applied
    * @return a SiriusResult wrapping the result of the operation.
    *         This should almost always be SiriusResult.none().
    *         In the future the API may be modified to return void.
    */
  override def handleDelete(key: String): SiriusResult = SiriusResult.none()
}
