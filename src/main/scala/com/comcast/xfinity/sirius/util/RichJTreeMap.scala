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
package com.comcast.xfinity.sirius.util

import collection.JavaConversions.asScalaIterator
import java.util.{TreeMap => JTreeMap}
import scala.util.control.Breaks._

object RichJTreeMap {

  /**
   * Create a RichJTreeMap and populate with provided
   * elements
   *
   * @kvs varargs of key/value pairs, same as you would
   *        construct standard Scala Maps
   */
  def apply[K, V](kvs: (K, V)*): RichJTreeMap[K, V] = {
    val map = new RichJTreeMap[K, V]
    kvs.foreach(kv => map.put(kv._1, kv._2))
    map
  }

  /**
   * Create a RichJTreeMap and populate with elements
   * from an existing Scala Map
   *
   * @param from Map to populate instance from
   */
  def apply[K, V](from: Map[K, V]): RichJTreeMap[K, V] = apply(from.toSeq: _*)
}

/**
 * A Java TreeMap with some functional style helpers
 * for mutating the underling collection (contradictory eh?)
 *
 * The JavaConversions stuff doesn't appear to have anything
 * that allows us to mutate the underlying collection
 */
class RichJTreeMap[K, V] private extends JTreeMap[K, V] {

  /**
   * Apply an operation to each element in order
   *
   * @param fun function to execute on each entry
   */
  def foreach(fun: (K, V) => Unit) {
    asScalaIterator(entrySet.iterator).foreach(
      entry => fun(entry.getKey, entry.getValue)
    )
  }

  /**
   * Remove all elements from this collection not
   * satisfying the predicate function
   *
   * @param predicate a function to be applied to each
   *        key value pair in the map. Only elements which
   *        for which this function evaluates to true are
   *        retained
   */
  def filter(predicate: (K, V) => Boolean) {
    var toDelete = List[K]()
    foreach(
      (k, v) =>
        if (!predicate(k, v))
          toDelete = k :: toDelete
    )
    toDelete.foreach(remove(_))
  }

  /**
   * Remove elements from the beginning of the collection
   * until predicate evaluates to false
   *
   * @param predicate a function to be applied to each
   *        key value pair in the map. All elements for
   *        which this function evaulates true up to
   *        the first element (not inclusive) for which
   *        it returns false are removed.
   */
  def dropWhile(predicate: (K, V) => Boolean) {
    var toDelete = List[K]()
    breakable {
      foreach(
        (k, v) =>
          if (predicate(k, v))
            toDelete = k :: toDelete
          else
            break()
      )
    }
    toDelete.foreach(remove(_))
  }
}
