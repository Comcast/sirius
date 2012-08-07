package com.comcast.xfinity.sirius.writeaheadlog

import org.slf4j.LoggerFactory

/**
 * Mixin for Checksumming. Uses FNV-1a hash
 * (see http://en.wikipedia.org/wiki/Fowler_Noll_Vo_hash#FNV-1_hash)
 */
trait Checksum {
  final val CHECKSUM_LENGTH = 16
  final val fnvOffsetBasis = -3750763034362895579L
  final val fnvPrime = 1099511628211L

  /**
   * Generates FNV-1a checksum for data
   *
   * @param data String to generate hash for, based on bytes
   *
   * @return the fnv-1a hash
   */
  def generateChecksum(data: String): String = "%016x".format(fnv1a(data))

  /**
   * Validate that the checksum of data matches the passed in checksum
   *
   * @param data String to generate checksum for, and compare such against
   *            the expected checksum
   * @param checksum the expected resultant checksum of data
   *
   * @return true if data's checksum matches checksum, false if not
   */
  def validateChecksum(data: String, checksum: String): Boolean = {
    val expectedChecksum = generateChecksum(data)
    checksum.equals(expectedChecksum)
  }

  private def fnv1a(data: String): Long = {
    val bytes = data.getBytes
    var hash = fnvOffsetBasis
    var i = 0

    while (i < bytes.length) {
      hash = fnvPrime * (hash ^ (bytes(i) & 0x0ff))
      i += 1
    }

    hash
  }
}
