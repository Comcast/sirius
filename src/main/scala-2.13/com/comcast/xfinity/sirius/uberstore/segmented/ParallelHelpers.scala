package com.comcast.xfinity.sirius.uberstore.segmented

import scala.collection.parallel.CollectionConverters._
import scala.collection.parallel.ParSeq

object ParallelHelpers {
    def parallelize[T](seq: Seq[T]): ParSeq[T] = seq.par
}
