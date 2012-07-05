package com.comcast.xfinity.sirius.api.impl

class SiriusState {
  var supervisorState = SiriusState.SupervisorState.Uninitialized
  var persistenceState = SiriusState.PersistenceState.Uninitialized
  var stateActorState = SiriusState.StateActorState.Uninitialized

  def updateSupervisorState(state: SiriusState.SupervisorState.Value): SiriusState = {
    supervisorState = state
    this
  }

  def updatePersistenceState(state: SiriusState.PersistenceState.Value): SiriusState = {
    persistenceState = state
    this
  }

  def updateStateActorState(state: SiriusState.StateActorState.Value): SiriusState = {
    stateActorState = state
    this
  }
}


object SiriusState {
  object StateActorState extends Enumeration {
    type State = Value
    val Uninitialized, Initialized = Value
  }

  object SupervisorState extends Enumeration {
    type State = Value
    val Uninitialized, Initialized = Value
  }

  object PersistenceState extends Enumeration {
    type State = Value
    val Uninitialized, Initialized = Value
  }
}