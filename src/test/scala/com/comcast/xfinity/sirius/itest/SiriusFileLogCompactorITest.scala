package com.comcast.xfinity.sirius.itest

import java.io.File
import com.comcast.xfinity.sirius.writeaheadlog.SiriusFileLog
import com.comcast.xfinity.sirius.writeaheadlog.SiriusFileLogCompactor
import com.comcast.xfinity.sirius.writeaheadlog.WriteAheadLogSerDe
import com.comcast.xfinity.sirius.NiceTest
import com.comcast.xfinity.sirius.api.impl.{Delete, OrderedEvent, Put}

class SiriusFileLogCompactorITest extends NiceTest {
  var logCompactor: SiriusFileLogCompactor = _
  var log: SiriusFileLog = _

  var FILE: File = _

  before {
    FILE = File.createTempFile("sirius", ".log")
    log = new SiriusFileLog(FILE.getAbsolutePath, new WriteAheadLogSerDe())
    logCompactor = new SiriusFileLogCompactor(log)
  }

  after {
    FILE.deleteOnExit();
  }

  describe(".compactLog") {
    it("compacting an empty file returns an empty list of compacted log entries") {
      val listOfEntries = logCompactor.compactLog()  
      assert(listOfEntries == List())
    }

    it("compacting a log file with one entry returns a list of one") {
      val entry = OrderedEvent(1, 300392, Put("A", "A".getBytes))

      log.writeEntry(entry)
      val listOfEntries = logCompactor.compactLog()
      assert(listOfEntries === List(entry))
    }

    it("compacting a log file with multiple PUT entries with the same key returns a list of one entry with one key") {
      val entry1 = OrderedEvent(1, 300392, Put("key1", "a".getBytes))
      val entry2 = OrderedEvent(2, 300392, Put("key1", "a".getBytes))
      val entry3 = OrderedEvent(3, 300392, Put("key1", "a".getBytes))

      log.writeEntry(entry1)
      log.writeEntry(entry2)
      log.writeEntry(entry3)

      val listOfEntries = logCompactor.compactLog()
      assert(listOfEntries === List(entry3))
    }

    it("compacting a log file with multiple entries with the same key where last entry is a DELETE, " +
       "returns a list of one DELETE entry") {
      val entry1 = OrderedEvent(1, 300392, Put("key1", "a".getBytes))
      val entry2 = OrderedEvent(2, 300392, Put("key1", "a".getBytes))
      val entry3 = OrderedEvent(3, 300392, Delete("key1"))

      log.writeEntry(entry1)
      log.writeEntry(entry2)
      log.writeEntry(entry3)

      val listOfEntries = logCompactor.compactLog()
      assert(listOfEntries === List(entry3))
    }

    it("compacting a log file with multiple entries with two different keys returns a list of two entries") {
      val entry1 = OrderedEvent(1, 300392, Put("key1", "a".getBytes))
      val entry2 = OrderedEvent(2, 300392, Delete("key1"))
      val entry3 = OrderedEvent(3, 300392, Put("key2", "a".getBytes))

      log.writeEntry(entry1)
      log.writeEntry(entry2)
      log.writeEntry(entry3)

      val listOfEntries = logCompactor.compactLog()
      assert(listOfEntries === List(entry2, entry3))
    }
  
    it("compacting a log file with multiple entries with n different keys returns a list of n entries") {
      val entry1 = OrderedEvent(1, 300392, Put("key1", "a".getBytes))
      val entry2 = OrderedEvent(2, 300392, Delete("key2"))
      val entry3 = OrderedEvent(3, 300392, Put("key3", "b".getBytes))

      log.writeEntry(entry1)
      log.writeEntry(entry2)
      log.writeEntry(entry3)

      val listOfEntries = logCompactor.compactLog()
      assert(listOfEntries === List(entry1, entry2, entry3))
    }

    it("compacting a log file with multiple unordered entries with n different keys returns an " +
       "ordered list of n entries by sequence number") {
      val entry1 = OrderedEvent(3, 300392, Put("key1", "a".getBytes))
      val entry2 = OrderedEvent(1, 300392, Delete("key2"))
      val entry3 = OrderedEvent(2, 300392, Put("key3", "b".getBytes))

      log.writeEntry(entry1)
      log.writeEntry(entry2)
      log.writeEntry(entry3)

      val listOfEntries = logCompactor.compactLog()
      assert(listOfEntries === List(entry2, entry3, entry1))
    }
  }
}