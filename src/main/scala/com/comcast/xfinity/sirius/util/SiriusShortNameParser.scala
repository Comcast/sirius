/*
 *  Copyright 2012-2014 Comcast Cable Communications Management, LLC
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.comcast.xfinity.sirius.util

import scala.util.parsing.combinator.RegexParsers

/**
 * Parser for translating short representation of Sirius node names to
 * their full Akka address string.
 *
 * It uses defaults for fields which aren't provided.  Below are some
 * example inputs with outputs:
 *
 *  host                  -> akka://sirius-system@host:2552/user/sirius
 *  host:1234             -> akka://sirius-system@host:1234/user/sirius
 *  system@host           -> akka://system@host:2552/user/sirius
 *  system@host:1234      -> akka://system@host:1234/user/sirius
 *  host/path             -> akka://sirius-system@host:2552/path
 *  host:1234/path        -> akka://sirius-system@host:1234/path
 *  system@host:1234/path -> akka://system@host:1234/path
 *  /local/path           -> /local/path
 *
 * Anything prefixed with "akka://" will be passed through without any
 * parsing.
 */
object SiriusShortNameParser extends RegexParsers {

  // Case classes used as intermediaries, probably not the cleanest,
  //  but works
  private case class HostPort(hostStr: String, portNum: Int)
  private case class SysHostPort(systemStr: String, hostPort: HostPort)
  private case class SysHostPortPath(shp: SysHostPort, pathStr: String)

  private def host: Parser[String] = "[0-9a-zA-Z][0-9a-zA-Z-\\.]*".r
  private def port: Parser[Int] = "[0-9]+".r ^^ {(p: String) => p.toInt}
  private def system: Parser[String] = "[a-zA-Z0-9][a-zA-Z0-9-]*".r
  private def path: Parser[String] = "(/[a-zA-Z\\$0-9]+)+".r

  private def hostPort: Parser[HostPort] =
    host ~ opt(":" ~ port) ^^ {
      case hostStr ~ Some(":" ~ portNum) => HostPort(hostStr, portNum)
      case hostStr ~ None => HostPort(hostStr, 2552)
    }

  private def systemHostPort: Parser[SysHostPort] =
    opt(system ~ "@") ~ hostPort ^^ {
      case Some(sysString ~ "@") ~ hp => SysHostPort(sysString, hp)
      case None ~ hp => SysHostPort("sirius-system", hp)
    }

  private def systemHostPortPath: Parser[SysHostPortPath] =
    systemHostPort ~ opt(path) ^^ {
      case shp ~ Some(pathStr) => SysHostPortPath(shp, pathStr)
      case shp ~ None => SysHostPortPath(shp, "/user/sirius")
    }

  private def shortAddress: Parser[String] =
    systemHostPortPath ^^ {
      case SysHostPortPath(
        SysHostPort(sysName,
          HostPort(hostName, portNum)
        ),
        path) => "akka://%s@%s:%d%s".format(sysName, hostName, portNum, path)
    } | path


  /**
   * Parse the input short representation of the address and return the result.
   *
   * @param shortName short name to be expanded, see class documentation for more
   *          info
   * @return Some(expandedAddressString) if parsing is successful
   *         None if parsing failed
   */
  // XXX: is Option really the right return?
  def parse(shortName: String): Option[String] = {
    if (shortName.startsWith("akka://")) {
      Some(shortName)
    } else {
      parseAll(shortAddress, shortName) match {
        case Success(v, _) => Some(v)
        case f => None
      }
    }
  }
}
