package com.comcast.xfinity.sirius.uberstore.segmented

import java.io.File

object FlagFile {
  /**
   * Instantiate a persistent boolean flag at the given location.
   *
   * @param location String location of flag
   */
  def apply(location: String) = {
    new FlagFile(new File(location))
  }
}

/**
 * Persistent boolean flag at the given location.
 *
 * @param location location of flag
 */
private[segmented] class FlagFile(location: File) {
  /**
   * Get current value of flag.
   * @return
   */
  def value = location.exists

  /**
   * Set the value of the flag.
   * @param value new value
   */
  def set(value: Boolean) {
    value match {
      case true =>
        location.createNewFile()
      case false =>
        location.delete()
    }
  }
}
