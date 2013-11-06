package com.comcast.xfinity.sirius.api

import com.comcast.xfinity.sirius.NiceTest

class SiriusConfigurationTest extends NiceTest {

  it ("must be able to reliably store arbitrary data") {
    val underTest = new SiriusConfiguration()

    assert(None === underTest.getProp("hello"))
    underTest.setProp("hello", 1)
    assert(Some(1) === underTest.getProp("hello"))

    underTest.setProp("world", "asdf")
    assert(Some("asdf") === underTest.getProp[String]("world"))
  }

  it ("must return data directly, or a default if provided") {
    val underTest = new SiriusConfiguration()

    assert("bar" === underTest.getProp("hello", "bar"))
    underTest.setProp("hello", "world")
    assert("world" === underTest.getProp("hello", "not-world"))
  }
}