package com.comcast.xfinity.sirius.uberstore

import com.comcast.xfinity.sirius.NiceTest
import com.comcast.xfinity.sirius.api.impl.{OrderedEvent, Delete}

class StaticEventIteratorTest extends NiceTest {

  it ("must intuitively wrap the passed in List") {
    val event1 = OrderedEvent(1, 2, Delete("duck"))
    val event2 = OrderedEvent(2, 3, Delete("duck"))
    val event3 = OrderedEvent(3, 4, Delete("goose"))

    val underTest = new StaticEventIterator(List(event1, event2, event3))

    assert(true === underTest.hasNext)
    assert(event1 === underTest.next())

    assert(true === underTest.hasNext)
    assert(event2 === underTest.next())

    assert(true === underTest.hasNext)
    assert(event3 === underTest.next())

    assert(false === underTest.hasNext)
    intercept[IllegalStateException] {
      underTest.next()
    }
  }

  it ("must stop iterating when closed") {
    val events = List(OrderedEvent(1, 2, Delete("BOOM")))
    val underTest = new StaticEventIterator(events)

    assert(true === underTest.hasNext)
    assert(Nil === underTest.close())
    assert(false === underTest.hasNext)
    intercept[IllegalStateException] {
      underTest.next()
    }
  }
}