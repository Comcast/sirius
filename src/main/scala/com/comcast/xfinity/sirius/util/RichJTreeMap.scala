package com.comcast.xfinity.sirius.util

import collection.JavaConversions.asScalaIterator
import java.util.{TreeMap => JTreeMap}
import scala.util.control.Breaks._

/**
 * A Java TreeMap with some functional style helpers
 * for mutating the underling collection (contradictory eh?)
 *
 * The JavaConversions stuff doesn't appear to have anything
 * that allows us to mutate the underlying collection
 */
class RichJTreeMap[K, V] extends JTreeMap[K, V] {

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