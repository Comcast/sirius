package com.comcast.xfinity.sirius.api.impl
import org.junit.runner.RunWith
import org.junit.Before
import org.mockito.runners.MockitoJUnitRunner
import org.mockito.Matchers
import org.mockito.Mock
import org.mockito.Mockito
import org.mockito.Spy
import com.comcast.xfinity.sirius.api.RequestHandler
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Props
import akka.testkit.TestProbe
import org.junit.After
import org.junit.Test
import com.comcast.xfinity.sirius.api.RequestMethod

@RunWith(classOf[MockitoJUnitRunner])
class SiriusScalaImplTest {
  
  @Mock
  var mockRequestHandler: RequestHandler = _
  
  var actorSystem: ActorSystem = _
  
  var stateWorkerProbe: TestProbe = _
  
  var underTest: SiriusScalaImpl = _
  
  @Before
  def setUp() = {
    actorSystem = Mockito.spy(ActorSystem("test"))
    
    stateWorkerProbe = TestProbe()(actorSystem)
    Mockito.doReturn(stateWorkerProbe.ref).when(actorSystem).actorOf(Matchers.any(classOf[Props]))
    underTest = new SiriusScalaImpl(mockRequestHandler, actorSystem)
  }
  
  @After
  def tearDown = {
    actorSystem.shutdown()
  }
  
  @Test
  def testEnqueuePutForwardsProperMessageToStateWorker() = {
    val key = "hello"
    val body = "there".getBytes()
    underTest.enqueuePut(key, body)
    stateWorkerProbe.expectMsg((RequestMethod.PUT, key, body))
    ()
  }
  
  @Test
  def testEnqueueGetForwardsProperMessageToStateWorker() = {
    val key = "hello"
    underTest.enqueueGet(key)
    stateWorkerProbe.expectMsg((RequestMethod.GET, key, null))
    ()
  }
  
}