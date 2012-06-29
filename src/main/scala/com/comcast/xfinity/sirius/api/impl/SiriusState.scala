package com.comcast.xfinity.sirius.api.impl

class SiriusState {
  var supervisorState = SiriusState.SupervisorState.Uninitialized
  var persistenceState = SiriusState.PersistenceState.Uninitialized

  def updateSupervisorState(state: SiriusState.SupervisorState.Value): SiriusState = {
    supervisorState = state
    this
  }

  def updatePersistenceState(state: SiriusState.PersistenceState.Value): SiriusState = {
    persistenceState = state
    this
  }
}

object SiriusState {
  object SupervisorState extends Enumeration {
    type State = Value
    val Uninitialized, Initialized = Value
  }

  object PersistenceState extends Enumeration {
    type State = Value
    val Uninitialized, Initialized = Value
  }
}