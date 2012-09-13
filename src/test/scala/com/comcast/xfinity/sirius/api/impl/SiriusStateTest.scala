package com.comcast.xfinity.sirius.api.impl

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import com.comcast.xfinity.sirius.NiceTest

@RunWith(classOf[JUnitRunner])
class SiriusStateTest extends NiceTest {

  describe("a SiriusState") {
    it ("should be able to tell when all subsystems are online") {
      val underTest = new SiriusState

      assert(false === underTest.areSubsystemsInitialized)

      assert(false === underTest.copy(
        stateInitialized = true
      ).areSubsystemsInitialized)

      assert(false === underTest.copy(
        stateInitialized = true,
        persistenceInitialized = true
      ).areSubsystemsInitialized)

      assert(true === underTest.copy(
        stateInitialized = true,
        persistenceInitialized = true,
        membershipInitialized = true
      ).areSubsystemsInitialized)
    }
  }
}