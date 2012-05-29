package com.comcast.xfinity.sirius.admin
import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfter
import org.scalatest.FunSpec
import org.scalatest.junit.JUnitRunner
import javax.management.MBeanServer
import org.mockito.Mockito._
import org.mockito.Matchers._
import javax.management.ObjectName

@RunWith(classOf[JUnitRunner])
class SiriusAdminTest extends FunSpec with BeforeAndAfter  {

  var mockMBeanServer : MBeanServer = _
  
  val mbeanName = new ObjectName("com.comcast.xfinity.sirius:type=SiriusInfo")
  var siriusAdminUnderTest : SiriusAdmin = _
  
  before {
    mockMBeanServer = mock(classOf[MBeanServer])
    siriusAdminUnderTest = new SiriusAdmin(4242, mockMBeanServer)
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