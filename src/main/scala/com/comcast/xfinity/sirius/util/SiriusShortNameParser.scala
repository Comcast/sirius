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
 * All of the above inputs may also be prefixed with "akka://" with the
 * exception of /local/path, which is a special case that allows us to
 * still supply a local path.
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

  private def akkaAddress: Parser[String] =
    opt("akka://") ~> systemHostPortPath ^^ {
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
  def parse(shortName: String): Option[String] =
    parseAll(akkaAddress, shortName) match {
      case Success(v, _) => Some(v)
      case f => None
    }
}
