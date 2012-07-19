package com.comcast.xfinity.sirius.api.impl.paxos

import org.scalatest.BeforeAndAfterAll
import com.comcast.xfinity.sirius.api.impl.paxos.PaxosMessages._
import com.comcast.xfinity.sirius.NiceTest
import akka.agent.Agent
import akka.testkit.TestActorRef
import akka.testkit.TestProbe
import org.mockito.Mockito._
import org.mockito.Matchers._
import akka.actor.{ActorRef, actorRef2Scala, ActorSystem}
import com.comcast.xfinity.sirius.api.impl.Delete
import collection.immutable.SortedMap

object LeaderTest {
  def makeMockedUpLeader(membership: Agent[Set[ActorRef]],
                         helper: LeaderHelper = mock(classOf[LeaderHelper]),
                         startScoutFun: => Unit = {},
                         startCommanderFun: (PValue) => Unit = p => {})
                        (implicit as: ActorSystem) = {
    TestActorRef(
      new Leader(membership) with Leader.HelperProvider {
        val leaderHelper = helper
        def startCommander(pval: PValue) { startCommanderFun(pval) }
        def startScout() { startScoutFun }
      }
    )
  }
}

class LeaderTest extends NiceTest with BeforeAndAfterAll {

  import LeaderTest._

  implicit val actorSystem = ActorSystem("LeaderTest")

  override def afterAll {
    actorSystem.shutdown()
  }

  describe("A Leader") {
    describe("on instantiation") {
      it("must spin up a scout") {
        val membership = Agent(Set[ActorRef]())
        val scoutProbe = TestProbe()

        makeMockedUpLeader(
          membership,
          startScoutFun = { scoutProbe.ref ! 'hi }
        )

        scoutProbe.expectMsg('hi)
      }
    }

    describe("when receiving an Adopted message") {
      it("must start commanders for its proposals using the current ballot " +
         "and become active") {
        val mockHelper = mock[LeaderHelper]
        val membership = Agent(Set[ActorRef]())

        var pvalsCommandered = Set[PValue]()

        val leader = makeMockedUpLeader(
          membership,
          helper = mockHelper,
          startCommanderFun = (pval =>
            // ignore ts, because we don't care
            pvalsCommandered += PValue(pval.ballot, pval.slotNum, pval.proposedCommand)
          )
        )


        val proposals = SortedMap(
            (1L -> Command(null, 1, Delete("2"))),
            (2L -> Command(null, 2, Delete("3")))
          )

        doReturn(SortedMap[Long, Command]()).
          when(mockHelper).pmax(any(classOf[Set[PValue]]))
        doReturn(proposals).
          when(mockHelper).update(any(classOf[SortedMap[Long, Command]]),
                                  any(classOf[SortedMap[Long, Command]]))

        leader ! Adopted(leader.underlyingActor.ballotNum, Set())

        val expectedPvalsCommandered = proposals.foldLeft(Set[PValue]()){
          case (acc, (slot, cmd)) =>
            acc + PValue(leader.underlyingActor.ballotNum, slot, cmd)
        }

        assert(expectedPvalsCommandered === pvalsCommandered)
        assert(true === leader.underlyingActor.active)
      }
    }

    describe("when receiving a Propose message") {
      it ("must ignore such if a proposal already exists for this slot") {
        val mockHelper = mock[LeaderHelper]
        val membership = Agent(Set[ActorRef]())

        val leader = makeMockedUpLeader(
          membership,
          helper = mockHelper
        )

        leader.underlyingActor.proposals = SortedMap((1L -> Command(null, 2, Delete("3"))))

        intercept[MatchError] {
          leader.underlyingActor.receive(Propose(1, Command(null, 1, Delete("2"))))
        }
      }

      it ("must add the Command to its proposals, but not start a commander if not active") {
        val mockHelper = mock[LeaderHelper]
        val membership = Agent(Set[ActorRef]())

        var commanderStarted = false

        val leader = makeMockedUpLeader(
          membership,
          helper = mockHelper,
          startCommanderFun = p => commanderStarted = true
        )

        val slotNum = 1L
        val command = Command(null, 1, Delete("2"))

        leader ! Propose(slotNum, command)

        assert(Map((slotNum -> command)) === leader.underlyingActor.proposals)
        assert(false === commanderStarted)
      }

      it ("must add the Command to its proposals and start a commander if active") {
        val mockHelper = mock[LeaderHelper]
        val membership = Agent(Set[ActorRef]())

        var commanderStarted = false

        val leader = makeMockedUpLeader(
          membership,
          helper = mockHelper,
          startCommanderFun = p => commanderStarted = true
        )

        leader.underlyingActor.active = true

        val slotNum = 1L
        val command = Command(null, 1, Delete("2"))

        leader ! Propose(slotNum, command)

        assert(Map((slotNum -> command)) === leader.underlyingActor.proposals)
        assert(true === commanderStarted)
      }
    }

    describe("when receiving a Preempted message") {
      it ("must ignnore such if the attached Ballot is outdated") {
        val membership = Agent(Set[ActorRef]())

        val leader = makeMockedUpLeader(membership)

        leader.underlyingActor.ballotNum = Ballot(1, "asdf")

        intercept[MatchError] {
          leader.underlyingActor.receive(Preempted(Ballot(0, "asdf")))
        }
      }

      it ("must become inactive, and spawn a scout for a new, greater ballot") {
        val membership = Agent(Set[ActorRef]())

        var scoutStarted = false

        val leader = makeMockedUpLeader(
          membership,
          startScoutFun = { scoutStarted = true }
        )

        // will most probably be greater than the initial value :)
        val preemptingBallot = Ballot(1, "asdf")

        leader ! Preempted(preemptingBallot)

        assert(leader.underlyingActor.ballotNum > preemptingBallot)
        assert(false === leader.underlyingActor.active)
        assert(scoutStarted)
      }
    }

    describe("when receiving a ScoutTimeout") {
      it ("must start a new Scout but retain its current Ballot") {
        val membership = Agent(Set[ActorRef]())

        var scoutStarted = false

        val leader = makeMockedUpLeader(
          membership,
          startScoutFun = { scoutStarted = true }
        )

        val initialBallot = leader.underlyingActor.ballotNum

        leader ! ScoutTimeout

        assert(scoutStarted)
        assert(initialBallot === leader.underlyingActor.ballotNum)
      }
    }

    describe("when receiving a Reap message") {
      it ("must clean out all expired proposals and update its lowestAcceptableSlot") {
        val now = System.currentTimeMillis()
        val keepers = SortedMap[Long, Command](
          (4L -> Command(null, now, Delete("A"))),
          (5L -> Command(null, 1L, Delete("Z"))),
          (6L -> Command(null, now - 1000, Delete("B")))
        )

        val leader = makeMockedUpLeader(Agent(Set[ActorRef]()))

        leader.underlyingActor.proposals = SortedMap[Long, Command](
          (1L -> Command(null, now - (31 * 60 * 1000), Delete("C"))),
          (3L -> Command(null, 1, Delete("D")))
        ) ++ keepers

        leader ! Leader.Reap

        assert(4L === leader.underlyingActor.lowestAcceptableSlot)
        assert(keepers === leader.underlyingActor.proposals)
      }

      it ("must not update its lowestAcceptableSlotNumber if nothing is reaped") {
        val leader = makeMockedUpLeader(Agent(Set[ActorRef]()))

        leader.underlyingActor.proposals = SortedMap[Long, Command]()
        leader.underlyingActor.lowestAcceptableSlot = 10
        leader ! Leader.Reap
        assert(10 === leader.underlyingActor.lowestAcceptableSlot)

        leader.underlyingActor.proposals = SortedMap[Long, Command](
          11L -> Command(null, System.currentTimeMillis(), Delete("Z"))
        )
        leader ! Leader.Reap
        assert(10 === leader.underlyingActor.lowestAcceptableSlot)
      }
    }
  }

}