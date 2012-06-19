package com.comcast.xfinity.sirius.api

object SiriusResult {
  
  /**
   * Factory method for creating a SiriusResult with a value
   * 
   * @param value the value to contain within the constructed
   *        SiriusResult
   *        
   * @return SiriusResult
   */
  def some(value: Array[Byte]): SiriusResult = 
    new SiriusResult(Some(value))
  
  /**
   * Factory method for creating a SiriusResult with no value
   * 
   * @return SiriusResult 
   */
  def none(): SiriusResult = new SiriusResult(None)
  
}

/**
 * Class for wrapping results from Sirius.  This is meant to be an
 * easy to use from Java equivalent of the Scala Option class.
 * 
 * This should not be constructed directly, instead use the factory
 * methods {@link SiriusResult#some()} and {@link SiriusResult#none()}
 */
// TODO: hide this within the scope of the companion object?
class SiriusResult(value: Option[Array[Byte]]) {
  
  /**
   * Does this result contain a value?
   */
  def hasValue: Boolean = value != None
  
  /**
   * Retrieves the value of this result.  If no such result exists
   * an IllegalStateException is thrown.
   * 
   * @return the value wrapped by this instance if it exists
   * @throws IllegalStateException if no such value exists
   */
  def getValue: Array[Byte] = value match {
    case Some(v) => v
    case None => 
      throw new IllegalStateException("Result has no value")
  }
  
}