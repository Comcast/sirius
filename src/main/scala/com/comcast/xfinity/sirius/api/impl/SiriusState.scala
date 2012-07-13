package com.comcast.xfinity.sirius.api.impl

/**
 * Keeps track of what actors have been initalized.  
 * 
 * XXX: We should ditch this in favor of making SiriusSupervisor and its child supervisors
 * an FSM, which just change states based on message passing..  
 */
class SiriusState {
  var supervisorState = SiriusState.SupervisorState.Uninitialized
  var persistenceState = SiriusState.PersistenceState.Uninitialized
  var stateActorState = SiriusState.StateActorState.Uninitialized
  var membershipActorState = SiriusState.MembershipActorState.Uninitialized

  def updateSupervisorState(state: SiriusState.SupervisorState.Value): SiriusState = {
    supervisorState = state
    this
  }

  def updateMembershipActorState(state: SiriusState.MembershipActorState.Value): SiriusState = {
    membershipActorState = state
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
  object MembershipActorState extends Enumeration {
    type State = Value
    val Uninitialized, Initialized = Value
  }

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