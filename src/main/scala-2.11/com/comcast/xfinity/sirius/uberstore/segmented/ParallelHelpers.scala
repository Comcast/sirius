package com.comcast.xfinity.sirius.uberstore.segmented

import scala.collection.parallel.ParSeq

object ParallelHelpers {
    implicit class ParSeqConverter[T](private val seq: Seq[T]) {
        def parallelize: ParSeq[T] = seq.par
    }
}
