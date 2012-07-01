package com.comcast.xfinity.sirius.itest

import java.io.File
import com.comcast.xfinity.sirius.writeaheadlog.SiriusFileLog
import com.comcast.xfinity.sirius.writeaheadlog.SiriusFileLogCompactor
import com.comcast.xfinity.sirius.writeaheadlog.WriteAheadLogSerDe
import com.comcast.xfinity.sirius.NiceTest
import com.comcast.xfinity.sirius.writeaheadlog.LogData

class SiriusFileLogCompactorITest extends NiceTest {
  var logCompactor: SiriusFileLogCompactor = _
  var log: SiriusFileLog = _

  var FILENAME: File = _

  before {
    FILENAME = File.createTempFile("sirius", ".log")
    log = new SiriusFileLog(FILENAME.getAbsolutePath, new WriteAheadLogSerDe())
    logCompactor = new SiriusFileLogCompactor(log)
  }

  after {
    FILENAME.deleteOnExit();
  }

  describe(".compactLog") {
    it("compacting an empty file returns an empty list of compacted log entries") {
      val listOfEntries = logCompactor.compactLog()  
      assert(listOfEntries == List())
    }
  }
  
  describe(".compactLog") {
    it("compacting a log file with one entry returns a list of one") {
      val entry = new LogData("PUT", "key1", 1L, 300392L, Some(Array[Byte](65, 124, 65)));
      log.writeEntry(entry)
      val listOfEntries = logCompactor.compactLog()
      assert(listOfEntries === List(entry))
    }
  }
  
  describe(".compactLog") {
    it("compacting a log file with multiple PUT entries with the same key returns a list of one entry with one key") {
      val entry1 = new LogData("PUT", "key1", 1L, 300392L, Some(Array[Byte](65, 124, 65)));
      val entry2 = new LogData("PUT", "key1", 2L, 300392L, Some(Array[Byte](65, 124, 65)));
      val entry3 = new LogData("PUT", "key1", 3L, 300392L, Some(Array[Byte](65, 124, 65)));
      log.writeEntry(entry1)
      log.writeEntry(entry2)
      log.writeEntry(entry3)
      val listOfEntries = logCompactor.compactLog()
      assert(listOfEntries === List(entry3))
    }
  }
  
  describe(".compactLog") {
    it("compacting a log file with multiple entries with the same key where last entry is a DELETE, returns a list of one DELETE entry") {
      val entry1 = new LogData("PUT", "key1", 1L, 300392L, Some(Array[Byte](65, 124, 65)));
      val entry2 = new LogData("PUT", "key1", 2L, 300392L, Some(Array[Byte](65, 124, 65)));
      val entry3 = new LogData("DELETE", "key1", 3L, 300392L, Some(Array[Byte](65, 124, 65)));
      log.writeEntry(entry1)
      log.writeEntry(entry2)
      log.writeEntry(entry3)
      val listOfEntries = logCompactor.compactLog()
      assert(listOfEntries === List(entry3))
    }
  }
  
  describe(".compactLog") {
    it("compacting a log file with multiple entries with two different keys returns a list of two entries") {
      val entry1 = new LogData("PUT", "key1", 1L, 300392L, Some(Array[Byte](65, 124, 65)));
      val entry2 = new LogData("DELETE", "key1", 2L, 300392L, Some(Array[Byte](65, 124, 65)));
      val entry3 = new LogData("PUT", "key2", 3L, 300392L, Some(Array[Byte](65, 124, 65)));
      log.writeEntry(entry1)
      log.writeEntry(entry2)
      log.writeEntry(entry3)
      val listOfEntries = logCompactor.compactLog()
      assert(listOfEntries === List(entry2, entry3))
    }
  }
  
  describe(".compactLog") {
    it("compacting a log file with multiple entries with n different keys returns a list of n entries") {
      val entry1 = new LogData("PUT", "key1", 1L, 300392L, Some(Array[Byte](65, 124, 65)));
      val entry2 = new LogData("DELETE", "key2", 2L, 300392L, Some(Array[Byte](65, 124, 65)));
      val entry3 = new LogData("PUT", "key3", 3L, 300392L, Some(Array[Byte](65, 124, 65)));
      log.writeEntry(entry1)
      log.writeEntry(entry2)
      log.writeEntry(entry3)
      val listOfEntries = logCompactor.compactLog()
      assert(listOfEntries === List(entry1, entry2, entry3))
    }
  }
  
  describe(".compactLog") {
    it("compacting a log file with multiple unordered entries with n different keys returns an ordered list of n entries by sequence number") {
      val entry1 = new LogData("PUT", "key1", 3L, 300392L, Some(Array[Byte](65, 124, 65)));
      val entry2 = new LogData("DELETE", "key2", 1L, 300392L, Some(Array[Byte](65, 124, 65)));
      val entry3 = new LogData("PUT", "key3", 2L, 300392L, Some(Array[Byte](65, 124, 65)));
      log.writeEntry(entry1)
      log.writeEntry(entry2)
      log.writeEntry(entry3)
      val listOfEntries = logCompactor.compactLog()
      assert(listOfEntries === List(entry2, entry3, entry1))
    }
  }
}