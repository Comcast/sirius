package com.comcast.xfinity.sirius.writeaheadlog

import org.slf4j.LoggerFactory

/**
 * mixin for Checksumming.  Defaults to a base 64 encoded MD5 hash
 */
trait Checksum {
  val CHECKSUM_LENGTH = 16

  def generateChecksum(data: String): String = {
    "%016x".format(fnv1(data))
  }

  def validateChecksum(data: String, checksum: String): Boolean = {
    val expectedChecksum = generateChecksum(data)
    checksum.equals(expectedChecksum)
  }

  /**
   * Implementation of the FNV-1 hash
   *
   * http://en.wikipedia.org/wiki/Fowler_Noll_Vo_hash#FNV-1_hash
   */
  private def fnv1(data: String) = {
    val fnvOffsetBasis = -3750763034362895579L
    val fnvPrime = 1099511628211L
    
    data.getBytes.foldLeft(fnvOffsetBasis)((hash, b) => fnvPrime * (hash ^ (b.toLong & 0x00ff)))
  }
}
