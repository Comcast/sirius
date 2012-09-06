package com.comcast.xfinity.sirius.api.impl.paxos

import com.comcast.xfinity.sirius.NiceTest
import com.comcast.xfinity.sirius.api.impl.paxos.PaxosMessages._
import com.comcast.xfinity.sirius.api.impl.Delete
import collection.immutable.SortedMap
import java.util.{TreeMap => JTreeMap}
import scala.collection.JavaConversions._

class LeaderHelperTest extends NiceTest {

  val leaderHelper = new LeaderHelper()
  
  describe("LeaderHelper") {

    describe("update") {
      it ("must return a map containing all of the key/values in y, and all of the key/values in x without " +
          "a corresponding key in y") {
        val x = new JTreeMap[Long, Int](SortedMap((1L -> 5), (2L -> 3)))
        val y = new JTreeMap[Long, Int](SortedMap((1L -> 2), (3L -> 4)))

        val expected = new JTreeMap[Long, Int](SortedMap(
          (1L -> 2), (2L -> 3), (3L -> 4)
        ))

        assert(expected === leaderHelper.update(x, y))
      }
    }

    describe("pmax") {
      it ("must, for each unique slotNum in a Set[PValue], return the Map where keys are the slotNum, and the values " +
          "are those associated with the highest ballotNum for that given slotNum") {
        val pvals = Set(
          PValue(Ballot(1, "a"), 1, Command(null, 1234, Delete("1"))),
          PValue(Ballot(2, "a"), 1, Command(null, 12345, Delete("2"))),
          PValue(Ballot(1, "a"), 2, Command(null, 123, Delete("3")))
        )

        val expected = new JTreeMap[Long, Command](SortedMap[Long, Command](
          (1L -> Command(null, 12345, Delete("2"))),
          (2L -> Command(null, 123, Delete("3")))
        ))

        assert(expected === leaderHelper.pmax(pvals))
      }
    }
  }
}