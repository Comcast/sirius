package com.comcast.xfinity.sirius.writeaheadlog

import com.comcast.xfinity.sirius.NiceTest
import org.scalatest.BeforeAndAfterAll
import java.io.{FileNotFoundException, File}
import scalax.file.Path

object WalFileOpsTest {

  def stageFile(name: String, contents: String)(implicit tempDir: File): String = {
    val file = new File(tempDir, name)
    Path.fromString(file.getAbsolutePath).append(contents)
    file.getAbsolutePath
  }

}

class WalFileOpsTest extends NiceTest with BeforeAndAfterAll {

  import WalFileOpsTest._

  val underTest = new WalFileOps

  implicit val tempDir: File = {
    val tempDirName = "%s/sirius-fulltest-%s".format(
      System.getProperty("java.io.tmpdir"),
      System.currentTimeMillis()
    )
    val dir = new File(tempDirName)
    dir.mkdirs()
    dir
  }

  override def afterAll {
    tempDir.delete()
  }

  it ("must return None for a nonexistent file") {
    val fName = new File(tempDir, "doesntexist").getAbsolutePath

    assert(None === underTest.getLastLine(fName))
  }

  it ("must return None for a zero length file") {
    val file = stageFile("zero-length-file", "")

    assert(None === underTest.getLastLine(file))
  }

  it ("must return the contents of a 1 length file with no new line") {
    val file = stageFile("one-length-file", "c")

    assert(Some("c") === underTest.getLastLine(file))
  }

  it ("must return just new line for 1 length file with new line") {
    val file = stageFile("one-length-with-nl", "\n")

    assert(Some("\n") === underTest.getLastLine(file))
  }

  it ("must return just a new line for a 2 length file with 2 new lines") {
    val file = stageFile("two-length-all-nl", "\n\n")

    assert(Some("\n") === underTest.getLastLine(file))
  }

  it ("must return actual content with new line for single line file") {
    val file = stageFile("single-char-w-nl", "a\n")

    assert(Some("a\n") === underTest.getLastLine(file))
  }

  it ("must return the last line including the new line in a file with some meaningful content") {
    val file = stageFile("some-real-content", "hello\nworld\nhow\nare you\n")

    assert(Some("are you\n") === underTest.getLastLine(file))
  }
}