package com.comcast.xfinity.sirius.uberstore.common

/**
 * Trait supplying the FNV-1a checksum algorithm, as defined by
 *
 * http://en.wikipedia.org/wiki/Fowler%E2%80%93Noll%E2%80%93Vo_hash_function#FNV-1a_hash
 */
trait Fnv1aChecksummer extends Checksummer {
  final val fnvOffsetBasis = -3750763034362895579L
  final val fnvPrime = 1099511628211L

  /**
   * @inheritdoc
   */
  def checksum(bytes: Array[Byte]): Long = {
    var hash = fnvOffsetBasis
    var i = 0

    while (i < bytes.length) {
      hash = fnvPrime * (hash ^ (bytes(i) & 0x0ff))
      i += 1
    }

    hash
  }
}