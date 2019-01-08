/*
 *  Copyright 2012-2014 Comcast Cable Communications Management, LLC
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.comcast.xfinity.sirius.api.impl

import akka.actor.{ActorContext, ActorRef, ActorSystem}
import akka.testkit.{TestProbe, TestActorRef}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import akka.agent.Agent
import com.comcast.xfinity.sirius.{TimedTest, NiceTest}
import org.scalatest.BeforeAndAfterAll
import org.mockito.Mockito._
import com.comcast.xfinity.sirius.api.impl.SiriusSupervisor.CheckPaxosMembership
import com.comcast.xfinity.sirius.uberstore.CompactionManager.{CompactionMessage, Compact}
import com.comcast.xfinity.sirius.api.impl.membership.MembershipActor.{GetMembershipData, MembershipMessage}
import com.comcast.xfinity.sirius.api.impl.membership.MembershipHelper
import com.comcast.xfinity.sirius.api.impl.paxos.Replica
import com.comcast.xfinity.sirius.api.SiriusConfiguration
import com.comcast.xfinity.sirius.util.AkkaExternalAddressResolver

@RunWith(classOf[JUnitRunner])
class SiriusSupervisorTest extends NiceTest with BeforeAndAfterAll with TimedTest {

  def createSiriusSupervisor(stateAgent: Agent[SiriusState] = mock[Agent[SiriusState]],
                             membershipAgent: Agent[Map[String, Option[ActorRef]]] = mock[Agent[Map[String, Option[ActorRef]]]],
                             stateSup: ActorRef = TestProbe().ref,
                             membershipActor: ActorRef = TestProbe().ref,
                             stateBridge: ActorRef = TestProbe().ref,
                             statusSubsystem: ActorRef = TestProbe().ref,
                             paxosSupervisor: ActorRef = TestProbe().ref,
                             compactionManager: ActorRef = TestProbe().ref)(implicit actorSystem: ActorSystem) = {
    val testConfig = new SiriusConfiguration
    testConfig.setProp(SiriusConfiguration.AKKA_EXTERNAL_ADDRESS_RESOLVER,AkkaExternalAddressResolver(actorSystem)(testConfig))

    val childProvider = new SiriusSupervisor.ChildProvider(null, null, testConfig) {
      override def createStateAgent()(implicit context: ActorContext) = stateAgent
      override def createMembershipAgent()(implicit context: ActorContext) = membershipAgent
      override def createStateSupervisor(stateAgent: Agent[SiriusState])
                                        (implicit context: ActorContext) = stateSup
      override def createMembershipActor(membershipAgent: Agent[Map[String, Option[ActorRef]]])
                                        (implicit context: ActorContext) = membershipActor
      override def createStateBridge(stateSupervisor: ActorRef, siriusSupervisor: ActorRef, membershipHelper: MembershipHelper)
                                    (implicit context: ActorContext) = stateBridge
      override def createPaxosSupervisor(membershpip: MembershipHelper, performFun: Replica.PerformFun)
                                        (implicit context: ActorContext) = paxosSupervisor
      override def createStatusSubsystem(siriusSupervisor: ActorRef)(implicit context: ActorContext) = statusSubsystem
      override def createCompactionManager()(implicit context: ActorContext) = compactionManager
    }
    TestActorRef(new SiriusSupervisor(childProvider, testConfig))
  }

  implicit val actorSystem = ActorSystem("SiriusSupervisorTest")

  var paxosProbe: TestProbe = _
  var persistenceProbe: TestProbe = _
  var stateProbe: TestProbe = _
  var membershipProbe: TestProbe = _
  var compactionProbe: TestProbe = _
  var startStopProbe: TestProbe = _
  var mockMembershipAgent: Agent[Map[String, Option[ActorRef]]] = mock[Agent[Map[String, Option[ActorRef]]]]

  var supervisor: TestActorRef[SiriusSupervisor] = _

  override def afterAll() {
    actorSystem.terminate()
  }

  before {

    paxosProbe = TestProbe()
    persistenceProbe = TestProbe()
    stateProbe = TestProbe()
    membershipProbe = TestProbe()
    compactionProbe = TestProbe()
    startStopProbe = TestProbe()

    supervisor = createSiriusSupervisor(stateAgent = Agent(new SiriusState)(actorSystem.dispatcher),
                                        membershipAgent = mockMembershipAgent,
                                        stateSup = stateProbe.ref,
                                        membershipActor = membershipProbe.ref,
                                        paxosSupervisor = paxosProbe.ref,
                                        compactionManager = compactionProbe.ref)

    doReturn(Map()).when(mockMembershipAgent).get()
  }

  def initializeSupervisor(supervisor: TestActorRef[SiriusSupervisor]) {
    val siriusStateAgent = supervisor.underlyingActor.siriusStateAgent
    siriusStateAgent send SiriusState(supervisorInitialized = false, stateInitialized = true)
    // wait for agent to get updated, just in case
    assert(waitForTrue(siriusStateAgent().areSubsystemsInitialized, 1000, 100))

    val senderProbe = TestProbe()
    senderProbe.send(supervisor, SiriusSupervisor.IsInitializedRequest)
    senderProbe.expectMsg(SiriusSupervisor.IsInitializedResponse(initialized = true))
  }

  describe("a SiriusSupervisor") {
    it("should start in the uninitialized state") {
      val siriusState = supervisor.underlyingActor.siriusStateAgent()
      assert(false === siriusState.supervisorInitialized)
    }

    it("should transition into the initialized state") {
      initializeSupervisor(supervisor)
      val stateAgent = supervisor.underlyingActor.siriusStateAgent
      waitForTrue(stateAgent().supervisorInitialized, 5000, 250)
    }

    it("should forward MembershipMessages to the membershipActor") {
      initializeSupervisor(supervisor)
      initializeOrdering(supervisor, Some(paxosProbe.ref))

      val membershipMessage: MembershipMessage = GetMembershipData
      supervisor ! membershipMessage
      membershipProbe.expectMsg(membershipMessage)
    }

    it("should forward GET messages to the stateActor") {
      initializeSupervisor(supervisor)
      initializeOrdering(supervisor, Some(paxosProbe.ref))

      val get = Get("1")
      supervisor ! get
      stateProbe.expectMsg(get)
    }
    
    it("should forward DELETE messages to the paxosActor") {
      doReturn(Map("sirius" -> Some(supervisor))).when(mockMembershipAgent).get()
      initializeSupervisor(supervisor)
      initializeOrdering(supervisor, Some(paxosProbe.ref))

      val delete = Delete("1")
      supervisor ! delete
      paxosProbe.expectMsg(delete)
    }

    def initializeOrdering(supervisor: TestActorRef[SiriusSupervisor], expectedActor: Option[ActorRef]) {
      supervisor ! CheckPaxosMembership
      waitForTrue(supervisor.underlyingActor.orderingActor == expectedActor, 1000, 100)
    }

    it("should forward PUT messages to the paxosActor") {
      doReturn(Map("sirius" -> Some(supervisor))).when(mockMembershipAgent).get()
      initializeSupervisor(supervisor)
      initializeOrdering(supervisor, Some(paxosProbe.ref))

      val put = Put("1", "someBody".getBytes)
      supervisor ! put
      paxosProbe.expectMsg(put)
    }
    it("should not have a CompactionManager until after it is initialized"){
      doReturn(Map("sirius" -> Some(supervisor))).when(mockMembershipAgent).get()
      assert(None === supervisor.underlyingActor.compactionManager)
      initializeSupervisor(supervisor)
      assert(waitForTrue(supervisor.underlyingActor.compactionManager == Some(compactionProbe.ref), 1000, 100))
    }

    it("should forward compaction messages to the CompactionManager") {
      initializeSupervisor(supervisor)
      val compactionMessage: CompactionMessage = Compact
      supervisor ! compactionMessage
      compactionProbe.expectMsg(compactionMessage)
    }

    it("should start the orderingActor if it checks membership and it's in there") {
      doReturn(Map("sirius" -> Some(supervisor))).when(mockMembershipAgent).get()
      initializeSupervisor(supervisor)

      supervisor ! CheckPaxosMembership

      assert(waitForTrue(supervisor.underlyingActor.orderingActor == Some(paxosProbe.ref), 1000, 100))
    }

    it("should stop the orderingActor if it checks membership and it's not in there") {
      // start up OrderingActor
      doReturn(Map("sirius" -> Some(supervisor))).when(mockMembershipAgent).get()
      initializeSupervisor(supervisor)
      supervisor ! CheckPaxosMembership
      assert(waitForTrue(supervisor.underlyingActor.orderingActor == Some(paxosProbe.ref), 1000, 100))

      // so we can shut it down
      doReturn(Map()).when(mockMembershipAgent).get()
      initializeSupervisor(supervisor)
      supervisor ! CheckPaxosMembership
      assert(waitForTrue(supervisor.underlyingActor.orderingActor == None, 1000, 100))
    }

    it("should amend its orderingActor reference if it receives a matching Terminated message") {
      doReturn(Map("sirius" -> Some(supervisor))).when(mockMembershipAgent).get()
      initializeSupervisor(supervisor)
      supervisor ! CheckPaxosMembership

      assert(waitForTrue(supervisor.underlyingActor.orderingActor == Some(paxosProbe.ref), 1000, 100))

      actorSystem.stop(paxosProbe.ref)

      assert(waitForTrue(supervisor.underlyingActor.orderingActor == None, 1000, 100))
    }
  }
}
