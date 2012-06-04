package com.comcast.xfinity.sirius.admin
import org.junit.runner.RunWith
import org.mockito.Matchers.anyObject
import org.mockito.Mockito.verify

import com.comcast.xfinity.sirius.info.SiriusInfo
import com.comcast.xfinity.sirius.NiceTest

import javax.management.MBeanServer
import javax.management.ObjectName

class SiriusAdminTest extends NiceTest {

  var mockMBeanServer: MBeanServer = _
  var mockSiriusInfo: SiriusInfo = _

  val mbeanName = new ObjectName("com.comcast.xfinity.sirius:type=SiriusInfo")
  var siriusAdminUnderTest: SiriusAdmin = _

  before {
    mockMBeanServer = mock[MBeanServer]
    mockSiriusInfo = mock[SiriusInfo]
    siriusAdminUnderTest = new SiriusAdmin(mockSiriusInfo, mockMBeanServer)
  }

  describe("a SiriusAdmin") {
    it("registers its mbeans with the passed in MBeanServer when registerMbeans is called") {
      siriusAdminUnderTest.registerMbeans()
      verify(mockMBeanServer).registerMBean(anyObject(), org.mockito.Matchers.eq(mbeanName));
    }
    it("unregisters is mbeans with the passed in MBeanServer when unregisterMbeans is called") {
      siriusAdminUnderTest.unregisterMbeans()
      verify(mockMBeanServer).unregisterMBean(mbeanName);
    }
  }
}