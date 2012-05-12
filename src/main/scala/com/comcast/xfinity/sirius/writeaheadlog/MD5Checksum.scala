package com.comcast.xfinity.sirius.writeaheadlog

import org.apache.commons.codec.binary.Base64
import java.security.MessageDigest

trait MD5Checksum {

  private[writeaheadlog] var checksumCodec = new Base64();


  def generateChecksum(data: String): String = {
    val messageDigest: MessageDigest = getMessageDigest()
    val hash = messageDigest.digest(data.getBytes())
    checksumCodec.encodeToString(hash)
  }

  def validateChecksum(data: String, checksum: String): Boolean = {
    checksum.equals(generateChecksum(data))

  }

  def getMessageDigest(): MessageDigest = {
    MessageDigest.getInstance("MD5")
  }
}
