package com.comcast.xfinity.sirius.api.impl.paxos

import org.scalatest.BeforeAndAfterAll
import com.comcast.xfinity.sirius.{NiceTest, TimedTest}
import akka.agent.Agent
import akka.testkit.TestActorRef
import akka.testkit.TestProbe
import org.mockito.Mockito._
import org.mockito.Matchers._
import akka.actor._
import collection.immutable.SortedMap
import java.util.{TreeMap => JTreeMap}
import scala.collection.JavaConversions._
import com.comcast.xfinity.sirius.api.impl.paxos.PaxosMessages._
import scala.Some
import com.comcast.xfinity.sirius.api.impl.Delete
import com.comcast.xfinity.sirius.api.impl.paxos.LeaderWatcher.{Close, SeekLeadership}

class LeaderTest extends NiceTest with TimedTest with BeforeAndAfterAll {
  implicit val actorSystem = ActorSystem("LeaderTest")

  // XXX this should really be in the companion object, but we need the actorSystem
  //     defined in order to default things like Agents and TestProbes.  Need to figure
  //     out how to move it up without defining an ActorSystem in the companion object.
  def makeMockedUpLeader(membership: Agent[Set[ActorRef]] = Agent(Set[ActorRef]()),
                         startingSeqNum: Long = 1,
                         helper: LeaderHelper = mock[LeaderHelper],
                         startScoutFun: => Unit = {},
                         startCommanderFun: (PValue) => Unit = p => {}) = {
    TestActorRef(
      new Leader(membership, startingSeqNum) with Leader.HelperProvider {
        val leaderHelper = helper
        def startCommander(pval: PValue) { startCommanderFun(pval) }
        def startScout() { startScoutFun }
      }
    )
  }

  override def afterAll {
    actorSystem.shutdown()
  }

  describe("A Leader") {
    describe("on instantiation") {
      it("must spin up a scout") {
        val scoutProbe = TestProbe()

        makeMockedUpLeader(
          startScoutFun = { scoutProbe.ref ! 'hi }
        )

        scoutProbe.expectMsg('hi)
      }

      it("must set currentLeaderWatcher to None") {
        val leaderRef = makeMockedUpLeader()

        assert(None === leaderRef.underlyingActor.currentLeaderWatcher)
      }
    }

      it("must set electedLeaderBallot to None") {
        val leaderRef = makeMockedUpLeader()

        assert(None === leaderRef.underlyingActor.electedLeaderBallot)
      }
    }

    describe("when receiving an Adopted message") {
      it ("must start commanders for its proposals using the current ballot") {
        val mockHelper = mock[LeaderHelper]

        var pvalsCommandered = Set[PValue]()

        val leader = makeMockedUpLeader(
          helper = mockHelper,
          startCommanderFun = (pval =>
            // ignore ts, because we don't care
            pvalsCommandered += PValue(pval.ballot, pval.slotNum, pval.proposedCommand)
          )
        )


        val sProposals = SortedMap[Long, Command](
          (1L -> Command(null, 1, Delete("2"))),
          (2L -> Command(null, 2, Delete("3")))
        )
        val proposals = new JTreeMap[Long, Command](sProposals)

        doReturn(new JTreeMap[Long, Command]()).
          when(mockHelper).pmax(any(classOf[Set[PValue]]))
        doReturn(proposals).
          when(mockHelper).update(any(classOf[JTreeMap[Long, Command]]),
          any(classOf[JTreeMap[Long, Command]]))

        leader ! Adopted(leader.underlyingActor.ballotNum, Set())

        val expectedPvalsCommandered = sProposals.foldLeft(Set[PValue]()){
          case (acc, (slot, cmd)) =>
            acc + PValue(leader.underlyingActor.ballotNum, slot, cmd)
        }

        assert(expectedPvalsCommandered === pvalsCommandered)
      }

      it ("must set its electedLeaderBallot to its own current ballot") {
        val leader = makeMockedUpLeader()

        leader ! Adopted(leader.underlyingActor.ballotNum, Set())

        assert(Some(leader.underlyingActor.ballotNum) === leader.underlyingActor.electedLeaderBallot)
      }
    }

    describe("when receiving a Propose message") {
      it ("must ignore such if a proposal already exists for this slot") {
        val mockHelper = mock[LeaderHelper]

        val leader = makeMockedUpLeader(
          helper = mockHelper
        )

        val proposals = new JTreeMap[Long, Command](SortedMap(
          1L -> Command(null, 2, Delete("3"))
        ))
        leader.underlyingActor.proposals = proposals

        intercept[MatchError] {
          leader.underlyingActor.receive(Propose(1, Command(null, 1, Delete("2"))))
        }
      }

      it ("must add the Command to its proposals, but not start a commander if it " +
        "has no elected leader") {
        val mockHelper = mock[LeaderHelper]

        var commanderStarted = false

        val leader = makeMockedUpLeader(
          helper = mockHelper,
          startCommanderFun = p => commanderStarted = true
        )
        leader.underlyingActor.electedLeaderBallot = None

        val slotNum = 1L
        val command = Command(null, 1, Delete("2"))

        leader ! Propose(slotNum, command)

        assert(new JTreeMap[Long, Command](SortedMap(slotNum -> command)) === leader.underlyingActor.proposals)
        assert(false === commanderStarted)
      }

      it ("must not add the Command to its proposals or start a commander" +
        "if there exists an elected leader") {
        val mockHelper = mock[LeaderHelper]

        var commanderStarted = false

        val leader = makeMockedUpLeader(
          helper = mockHelper,
          startCommanderFun = p => commanderStarted = true
        )
        leader.underlyingActor.electedLeaderBallot = Some(Ballot(0, ""))

        val slotNum = 1L
        val command = Command(null, 1, Delete("2"))

        leader ! Propose(slotNum, command)

        assert(new JTreeMap[Long, Command]() === leader.underlyingActor.proposals)
        assert(false === commanderStarted)
      }

      it ("must add the Command to its proposals and start a commander if it is the leader") {
        val mockHelper = mock[LeaderHelper]

        var commanderStarted = false

        val leader = makeMockedUpLeader(
          helper = mockHelper,
          startCommanderFun = p => commanderStarted = true
        )

        leader.underlyingActor.electedLeaderBallot = Some(leader.underlyingActor.ballotNum)

        val slotNum = 1L
        val command = Command(null, 1, Delete("2"))

        leader ! Propose(slotNum, command)

        assert(new JTreeMap[Long, Command](SortedMap(slotNum -> command)) === leader.underlyingActor.proposals)
        assert(true === commanderStarted)
      }
    }

    describe("when receiving a Preempted message") {
      it ("must ignore such if the attached Ballot is outdated") {
        val leader = makeMockedUpLeader()

        leader.underlyingActor.ballotNum = Ballot(1, "asdf")

        intercept[MatchError] {
          leader.underlyingActor.receive(Preempted(Ballot(0, "asdf")))
        }
      }

      it ("must set its currentLeaderBallot") {
        val leader = makeMockedUpLeader()

        val preemptingBallot = Ballot(1, "asdf")

        leader ! Preempted(preemptingBallot)

        assert(Some(preemptingBallot) === leader.underlyingActor.electedLeaderBallot)
      }

      it ("must kill currentLeaderWatcher if it is running") {
        val watcherProbe = TestProbe()

        val leader = makeMockedUpLeader()
        leader.underlyingActor.currentLeaderWatcher = Some(watcherProbe.ref)

        val preemptingBallot = Ballot(1, "asdf")

        leader ! Preempted(preemptingBallot)

        watcherProbe.expectMsg(Close)
      }

      it ("must spawn a new currentLeaderWatcher with the current leader ballot") {
        val watcherProbe = TestProbe()

        val leader = makeMockedUpLeader()
        leader.underlyingActor.currentLeaderWatcher = Some(watcherProbe.ref)

        val preemptingBallot = Ballot(1, "asdf")

        assert(Some(watcherProbe.ref) === leader.underlyingActor.currentLeaderWatcher)

        leader ! Preempted(preemptingBallot)

        assert(Some(watcherProbe.ref) != leader.underlyingActor.currentLeaderWatcher)
        assert(!leader.underlyingActor.currentLeaderWatcher.get.isTerminated)
      }
    }

    describe("when receiving a SeekLeadership message") {
      it ("must set electedLeaderBallot to None") {
        val leader = makeMockedUpLeader()
        leader.underlyingActor.electedLeaderBallot = Some(Ballot(1, ""))

        leader ! SeekLeadership

        assert(None === leader.underlyingActor.electedLeaderBallot)
      }

      it ("must spawn a scout") {
        var scoutStarted = false

        val leader = makeMockedUpLeader(
          startScoutFun = { scoutStarted = true }
        )

        leader ! SeekLeadership

        assert(scoutStarted)
      }

      it ("must set its new ballot to one higher than its old ballot " +
        "if there was no previously elected leader") {
        val leader = makeMockedUpLeader()
        val oldBallot = Ballot(10, "a")

        leader.underlyingActor.ballotNum = oldBallot
        leader.underlyingActor.electedLeaderBallot = None

        leader ! SeekLeadership

        val newBallot = leader.underlyingActor.ballotNum
        assert(newBallot > oldBallot)
      }

      it ("must set its new ballot to one higher than both its own old ballot " +
        "AND the previous leader's ballot") {
        val leader = makeMockedUpLeader()
        val oldBallot = Ballot(10, "a")
        val electedBallot = Ballot(11, "b")

        leader.underlyingActor.ballotNum = oldBallot
        leader.underlyingActor.electedLeaderBallot = Some(electedBallot)

        leader ! SeekLeadership

        val newBallot = leader.underlyingActor.ballotNum
        assert(newBallot > oldBallot)
        assert(newBallot > electedBallot)
      }
    }

    describe("when receiving a ScoutTimeout") {
      it ("must do nothing if a leader has been elected in the meantime") {
        var scoutStarted = false

        val leader = makeMockedUpLeader(
          startScoutFun = { scoutStarted = true }
        )
        leader.underlyingActor.electedLeaderBallot = Some(Ballot(1, "asdf"))

        // have to reset it to false, scout is started on instantiation
        scoutStarted = false

        leader ! ScoutTimeout

        assert(scoutStarted == false)
      }

      it ("must start a new Scout but retain its current Ballot if " +
        "there is still no leader") {
        var scoutStarted = false

        val leader = makeMockedUpLeader(
          startScoutFun = { scoutStarted = true }
        )
        leader.underlyingActor.electedLeaderBallot = None

        val initialBallot = leader.underlyingActor.ballotNum

        leader ! ScoutTimeout

        assert(scoutStarted)
        assert(initialBallot === leader.underlyingActor.ballotNum)
      }
    }

    describe("when receiving a DecisionHint message") {
      it ("creates some decided slots, if not exist") {
        val leader = makeMockedUpLeader()

        leader ! DecisionHint(1L)

        assert(1L === leader.underlyingActor.latestDecidedSlot)
      }

      it ("creates adds to decided slots, if it exists") {
        val leader = makeMockedUpLeader()

        leader.underlyingActor.latestDecidedSlot = 1L

        leader ! DecisionHint(2L)

        assert(2L === leader.underlyingActor.latestDecidedSlot)
      }

      it ("must clean out all decided proposals") {
        val keepers = SortedMap[Long, Command](
          (4L -> Command(null, 2L, Delete("A"))),
          (5L -> Command(null, 1L, Delete("Z"))),
          (6L -> Command(null, 3L, Delete("B")))
        )

        val leader = makeMockedUpLeader(Agent(Set[ActorRef]()))

        leader.underlyingActor.proposals = new JTreeMap[Long, Command](
          SortedMap[Long, Command](
            (1L -> Command(null, 1, Delete("C"))),
            (3L -> Command(null, 1, Delete("D")))
          ) ++ keepers)

        leader.underlyingActor.latestDecidedSlot = 0L

        leader ! DecisionHint(3L)

        assert(new JTreeMap[Long, Command](keepers) === leader.underlyingActor.proposals)
      }

    }

    describe("when receiving a CommanderTimeout") {
      it ("must nullify the commandered slot and update its internal record keeping") {
        val leader = makeMockedUpLeader()

        // stage a proposal
        val slot = 1L
        val command = Command(null, 12345L, Delete("2"))
        leader.underlyingActor.proposals.put(slot, command)

        // get some information from before we fail
        val lastTimeoutCount = leader.underlyingActor.commanderTimeoutCount

        val pval = PValue(Ballot(1, "a"), slot, command)
        leader ! Commander.CommanderTimeout(pval)

        assert(!leader.underlyingActor.proposals.containsKey(slot))

        assert(lastTimeoutCount + 1 === leader.underlyingActor.commanderTimeoutCount)
        assert(Some(pval) === leader.underlyingActor.lastTimedOutPValue)
      }
    }
}