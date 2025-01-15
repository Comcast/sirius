package com.comcast.xfinity.sirius.uberstore.common

trait SkipValidationChecksummer extends Checksummer {
    /**
     * Skips calculating and validating the checksum on the Array[Byte]
     */
    override def validate(bytes: Array[Byte], chksum: Long): Boolean = true
}
