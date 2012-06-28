package com.comcast.xfinity.sirius.api.impl

class SiriusState {

  var persistenceActorState = PersistenceActorState.Uninitialized
}

object PersistenceActorState extends Enumeration {
  type State = Value
  val Uninitialized, Initialized = Value
}
