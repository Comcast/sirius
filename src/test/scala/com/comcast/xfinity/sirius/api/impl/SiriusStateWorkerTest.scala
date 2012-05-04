package com.comcast.xfinity.sirius.api.impl
import org.junit.runner.RunWith
import org.junit.Before
import org.junit.Test
import org.mockito.Mock
import org.mockito.Matchers
import org.mockito.Mockito
import com.comcast.xfinity.sirius.api.RequestHandler
import com.comcast.xfinity.sirius.api.RequestMethod
import akka.actor.ActorSystem
import akka.testkit.TestActorRef
import org.mockito.runners.MockitoJUnitRunner
import org.junit.After

@RunWith(classOf[MockitoJUnitRunner])
class SiriusStateWorkerTest {
  
  var actorSystem: ActorSystem = _
  
  @Mock
  var mockRequestHandler: RequestHandler = _
  
  var testActor: TestActorRef[SiriusStateWorker] = _
  
  var underTest: SiriusStateWorker = _
  
  @Before
  def setUp() = {
    actorSystem = ActorSystem("test")
    
    testActor = TestActorRef(new SiriusStateWorker(mockRequestHandler))(actorSystem)
    underTest = testActor.underlyingActor
  }
  
  @After
  def tearDown() = {
    actorSystem.shutdown()
  }
  
  @Test
  def testProperMessageForwardedToHandler() = {
    val method = RequestMethod.PUT
    val key = "key"
    val body = "value".getBytes()
    testActor ! (RequestMethod.PUT, key, body)
    Mockito.verify(mockRequestHandler).handle(Matchers.eq(method), Matchers.eq(key), Matchers.eq(body))
    ()
  }
  
  @Test
  def testInvalidMessageDoesNotInvokeHandler() = {
    testActor ! "derp"
    Mockito.verify(mockRequestHandler, Mockito.never()).
        handle(Matchers.any(classOf[RequestMethod]), Matchers.any(classOf[String]), Matchers.any(classOf[Array[Byte]]))
    ()
  }
}