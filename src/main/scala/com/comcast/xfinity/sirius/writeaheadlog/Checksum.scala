package com.comcast.xfinity.sirius.writeaheadlog

import org.apache.commons.codec.binary.Base64
import java.security.MessageDigest

/**
 * mixin for Checksumming.  Defaults to a base 64 encoded MD5 hash
 */
trait Checksum {

  private[writeaheadlog] var checksumCodec = new Base64();
  protected var checksumAlgorithm : String = "MD5"

  def generateChecksum(data: String): String = {
    val messageDigest: MessageDigest = getMessageDigest()
    val hash = messageDigest.digest(data.getBytes())
    checksumCodec.encodeToString(hash)
  }

  def validateChecksum(data: String, checksum: String): Boolean = {
    checksum.equals(generateChecksum(data))

  }

  def getMessageDigest(): MessageDigest = {
    MessageDigest.getInstance(checksumAlgorithm)
  }
}
