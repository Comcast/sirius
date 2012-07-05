package com.comcast.xfinity.sirius.api.impl

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import com.comcast.xfinity.sirius.NiceTest

@RunWith(classOf[JUnitRunner])
class SiriusStateTest extends NiceTest {

  var underTest: SiriusState = _

  before {
    underTest = new SiriusState
  }

  describe("a SiriusState") {
    it("should start out uninitialized") {
      assert(underTest.supervisorState === SiriusState.SupervisorState.Uninitialized)
      assert(underTest.persistenceState === SiriusState.PersistenceState.Uninitialized)
      assert(underTest.stateActorState === SiriusState.StateActorState.Uninitialized)
    }
    it("should update the supervisor state") {
      assert(underTest.supervisorState === SiriusState.SupervisorState.Uninitialized)
      underTest.updateSupervisorState(SiriusState.SupervisorState.Initialized)
      assert(underTest.supervisorState === SiriusState.SupervisorState.Initialized)
    }
    it("should update the persistence state") {
      assert(underTest.persistenceState === SiriusState.PersistenceState.Uninitialized)
      underTest.updatePersistenceState(SiriusState.PersistenceState.Initialized)
      assert(underTest.persistenceState === SiriusState.PersistenceState.Initialized)
    }
    it("should update the state actor state") {
      assert(underTest.stateActorState === SiriusState.StateActorState.Uninitialized)
      underTest.updateStateActorState(SiriusState.StateActorState.Initialized)
      assert(underTest.stateActorState === SiriusState.StateActorState.Initialized)
    }
  }
}