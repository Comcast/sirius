package com.comcast.xfinity.sirius.api.impl

/**
 * Keeps track of what actors have been initalized.  
 * 
 * XXX: We should ditch this in favor of making SiriusSupervisor and its child supervisors
 * an FSM, which just change states based on message passing..  
 */
case class SiriusState(supervisorInitialized: Boolean = false,
                       stateInitialized: Boolean = false,
                       membershipInitialized: Boolean = false) {

  def areSubsystemsInitialized = stateInitialized && membershipInitialized
}