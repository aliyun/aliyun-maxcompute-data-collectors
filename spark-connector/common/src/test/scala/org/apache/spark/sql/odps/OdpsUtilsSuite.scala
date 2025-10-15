package org.apache.spark.sql.odps

import org.scalatest.funsuite.AnyFunSuite

import java.io.IOException

class OdpsUtilsSuite extends AnyFunSuite {

  test("retryOnSpecificError") {
    val result = OdpsUtils.retryOnSpecificError(2, OdpsUtils.SESSION_ERROR_MESSAGE) {
      () => true
    }
    assert(result)

    intercept[UnsupportedOperationException] {
      OdpsUtils.retryOnSpecificError(2, OdpsUtils.SESSION_ERROR_MESSAGE) {
        () =>
          throw new UnsupportedOperationException("UnsupportedOperationException")
      }
    }

    intercept[UnsupportedOperationException] {
      OdpsUtils.retryOnSpecificError(2, OdpsUtils.SESSION_ERROR_MESSAGE) {
        () =>
          throw new UnsupportedOperationException()
      }
    }

    intercept[IOException] {
      OdpsUtils.retryOnSpecificError(2, OdpsUtils.SESSION_ERROR_MESSAGE) {
        () =>
          throw new IOException("OTSTimeout")
      }
    }
  }

}

