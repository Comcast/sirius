package com.comcast.xfinity.sirius.api

/**
 * Interface for a Sirius wrapped data structure for applying
 * operations to the in memory dataset.  All interactions with
 * this object should go through Sirius.  Operations should be
 * short and to the point and should at all costs not throw
 * exceptions.
 *
 * Access to this object is kept synchronized by Sirius.
 *
 * Puts and Deletes to the same key should cancel each other out.
 * Also successive puts should cancel each other out.
 */
trait RequestHandler {

  /**
   * Handle a GET request
   *
   * @param key String identifying the search query
   *
   * @return a SiriusResult wrapping the result of the query
   */
  def handleGet(key: String): SiriusResult

  /**
   * Handle a PUT request
   *
   * @param key unique identifier for the item to which the
   *          operation is being applied
   * @param body data passed in along with this request used
   *          for modifying the state at key
   *
   * @return a SiriusResult wrapping the result of the operation.
   *          This should almost always be SiriusResult.none().
   *          In the future the API may be modified to return void.
   */
  def handlePut(key: String, body: Array[Byte]): SiriusResult

  /**
   * Handle a DELETE request
   *
   * @param key unique identifier for the item to which the
   *          operation is being applied
   *
   * @return a SiriusResult wrapping the result of the operation.
   *          This should almost always be SiriusResult.none().
   *          In the future the API may be modified to return void.
   */
  def handleDelete(key: String): SiriusResult

}