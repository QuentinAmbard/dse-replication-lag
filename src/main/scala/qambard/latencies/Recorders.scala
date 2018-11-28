package qambard.latencies

import org.HdrHistogram.Histogram

object Recorders {
  val numberOfSignificantValueDigits = 3
  val highestTrackableValue = 60000

  val recorderReplication = new Histogram(highestTrackableValue, numberOfSignificantValueDigits)
  val recorderSelect = new Histogram(highestTrackableValue, numberOfSignificantValueDigits)
}
