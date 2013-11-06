package com.comcast.xfinity.sirius

import org.scalatest.{FunSpec, BeforeAndAfter}
import org.scalatest.mock.MockitoSugar
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
abstract class NiceTest extends FunSpec with BeforeAndAfter with MockitoSugar {

}