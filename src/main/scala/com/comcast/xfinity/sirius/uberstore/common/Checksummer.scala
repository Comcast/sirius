package com.comcast.xfinity.sirius.uberstore.common

/**
 * Trait supplying checksumming capabilities
 */
trait Checksummer {

  /**
   * Given an array of bytes will calculate a Long checksum
   * value for them
   *
   * @param bytes Array[Byte] to checksum
   *
   * @return the checksum as a Long
   */
  def checksum(bytes: Array[Byte]): Long
}