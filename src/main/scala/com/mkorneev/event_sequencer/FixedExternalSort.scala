package com.mkorneev.event_sequencer

import java.io.{File, IOException}
import java.util.Comparator

import com.google.code.externalsorting.ExternalSort

object FixedExternalSort {

  /**
    * This method in [[ExternalSort]] is broken. It doesn't propagate the comparator.
    */
  @throws[IOException]
  def sort(input: File, output: File, cmp: Comparator[String]): Unit = {
    ExternalSort.mergeSortedFiles(ExternalSort.sortInBatch(input, cmp), output, cmp)
  }

}
