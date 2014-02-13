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
  def some(value: Object): SiriusResult = SiriusResult(Right(Some(value)))
  
  /**
   * Factory method for creating a SiriusResult with no value
   * 
   * @return SiriusResult 
   */
  def none(): SiriusResult = SiriusResult(Right(None))
  
  /**
   * Factory method for creating a SiriusResult with an error.
   *
   * @param rte the RuntimeException to wrap
   *
   * @return SiriusResult 
   */  
  def error(rte: RuntimeException): SiriusResult = SiriusResult(Left(rte))
  
}

/**
 * Class for wrapping results from Sirius.  This is meant to be an
 * easy to use from Java equivalent of the Scala Option class.
 * 
 * This should not be constructed directly, instead use the factory
 * methods {@link SiriusResult#some()} and {@link SiriusResult#none()}
 */
// TODO: hide this within the scope of the companion object?
case class SiriusResult(private val value: Either[RuntimeException, Option[Object]]) {
  
  /**
   * Does this result contain a value?
   *
   * @return true if this instance wraps a value or exception
   */
  def hasValue: Boolean = value match {
    case Right(None) => false
    case _ => true
  }
  
  /**
   * Retrieves the value of this result.  If no such result exists
   * an IllegalStateException is thrown.
   * 
   * @return the value wrapped by this instance if it exists
   * @throws IllegalStateException if no such value exists
   */
  def getValue: Object = value match {
    case Left(rte) => throw rte
    case Right(Some(v)) => v
    case Right(None) => throw new IllegalStateException("Result has no value")
  }

  /**
   * @return true if this instance wraps an exception
   */
  def isError: Boolean = value.isLeft
}
