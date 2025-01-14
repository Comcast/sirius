package com.comcast.xfinity.sirius.uberstore.segmented

import scala.collection.parallel.immutable.ParSeq
import scala.collection.parallel.CollectionConverters._

object ParallelHelpers {
    def parallelize[T](seq: Seq[T]): ParSeq[T] = seq.par
}
