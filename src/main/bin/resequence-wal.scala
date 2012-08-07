import com.comcast.xfinity.sirius.api.impl.OrderedEvent
import com.comcast.xfinity.sirius.writeaheadlog.WriteAheadLogSerDe
import scalax.file.Path
import scalax.io.Line.Terminators.NewLine

/**
 * Only to be used on WAL files where sequence numbers are incorrect.
 * This script assumes that events are written in-order to file, but
 * sequence numbers are meaningless.  Rewrites entire file with new
 * sequence numbers, for use in particular with compaction.
 *
 * :load this file from sirius-interactive.sh
 *
 * @param inputFile
 * @param outputFile
 */
def resequence_wal(inputFile: String, outputFile: String) {
  val start = System.currentTimeMillis()

  val input = Path.fromString(inputFile)
  val output = Path.fromString(outputFile)
  val serde = new WriteAheadLogSerDe
  var seq = 1

  def rebuild_oe(line: String): String = {
    val oe = serde.deserialize(line)
    val newoe = OrderedEvent(seq, oe.timestamp, oe.request)
    serde.serialize(newoe)
  }

  val lines = input.lines(NewLine, includeTerminator=true)

  lines.foldLeft(())((acc, line) => {
    if (seq % 100000 == 0) print("\r"+seq+" lines written")

    val newline = rebuild_oe(line)
    output.append(newline)
    seq = seq + 1
  })

  val end = System.currentTimeMillis()

  println("Finished reordering in "+((end-start)/(1000))+" sec")

}
