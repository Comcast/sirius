package com.comcast.xfinity.sirius.util

import com.comcast.xfinity.sirius.NiceTest

class SiriusShortNameParserTest extends NiceTest {

  it ("must derive the full actor address with proper defaults when passed just host") {
    expect(Some("akka://sirius-system@hostname:2552/user/sirius")) {
      SiriusShortNameParser.parse("hostname")
    }
  }
  it ("must derive the full actor address with proper defaults when passed just host and port") {
    expect(Some("akka://sirius-system@hostname:1234/user/sirius")) {
      SiriusShortNameParser.parse("hostname:1234")
    }
  }

  it ("must derive the full actor address with proper defaults when passed just system and host") {
    expect(Some("akka://some-system@hostname:2552/user/sirius")) {
      SiriusShortNameParser.parse("some-system@hostname")
    }
  }

  it ("must derive the full actor address with proper defaults when passed just system, host, and port") {
    expect(Some("akka://some-system@hostname:1234/user/sirius")) {
      SiriusShortNameParser.parse("some-system@hostname:1234")
    }
  }

  it ("must derive the full actor address with proper defaults when passed just host and path") {
    expect(Some("akka://sirius-system@hostname:2552/sirius/jawn")) {
      SiriusShortNameParser.parse("hostname/sirius/jawn")
    }
  }

  it ("must derive the full actor address with proper defaults when passed just host, port, and path") {
    expect(Some("akka://sirius-system@hostname:1234/sirius/jawn")) {
      SiriusShortNameParser.parse("hostname:1234/sirius/jawn")
    }
  }

  it ("must derive the full actor address with proper defaults when passed just system, host, and path") {
    expect(Some("akka://some-system@hostname:2552/sirius/jawn")) {
      SiriusShortNameParser.parse("some-system@hostname/sirius/jawn")
    }
  }

  it ("must return the full actor address when the full address is passed in less the akka:// prefix") {
    expect(Some("akka://some-system@hostname:1234/sirius/jawn")) {
      SiriusShortNameParser.parse("some-system@hostname:1234/sirius/jawn")
    }
  }

  it ("must pass through anything prefixed with akka://") {
    expect(Some("akka://garbled-nonsense'dude...")) {
      SiriusShortNameParser.parse("akka://garbled-nonsense'dude...")
    }
  }

  it ("must return the just the path if just the path is provided") {
    expect(Some("/some/path")) {
      SiriusShortNameParser.parse("/some/path")
    }
  }

  it ("must return None for nonsense") {
    expect(None) {
      SiriusShortNameParser.parse("absolute nonsense")
    }
  }
}