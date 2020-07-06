package org.codefeedr.plugins.cargo.operators

import org.apache.flink.runtime.state.FunctionSnapshotContext
import org.scalatest.{BeforeAndAfter, FunSuite}

class CargoReleasesSourceTests extends FunSuite with BeforeAndAfter {

  val crs: CargoReleasesSource = new CargoReleasesSource()

  before {
    // First assure that the url field is set to the crates io summary stream
    assert(crs.url.equals("https://crates.io/api/v1/summary"))
  }

  test("getRSSAsString works on /api/v1/summary") {
    // Retrieve the RSS information in string format
    val cargoSummary = crs.getRSSAsString

    // Assert string type and correct start
    assert(cargoSummary.get.startsWith("{\"num_downloads\":"))
  }

  test("getRSSFromCrate works on example 'shtola-markdown'") {
    val shtolaCrate = crs.getRSSFromCrate("shtola-markdown")

    // Assert string type and correctly retrieved start of body
    assert(shtolaCrate.get.startsWith("{\"crate\":{\"id\":\"shtola-markdown\",\"name\":\"shtola-markdown\",\"updated_at\":\""))
  }

  test("snapshotState no lastItem") {
    // Arrange
    val ctx = new FunctionSnapshotContext {
      override def getCheckpointId: Long = 300

      override def getCheckpointTimestamp: Long = 200
    }

    // Act
    crs.snapshotState(ctx)

    // Assert
    assert(crs.getCheckpointedstate == null)
  }

  test("cancel success") {
    // Act
    crs.cancel()

    // Assert
    assert(!crs.getIsRunning)
  }

}
