package org.apache.spark.sql.odps

import org.apache.spark.{TaskContext, TaskContextImpl, TaskKilledException}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterEach

class OdpsTableDataWriterSuite extends AnyFunSuite with BeforeAndAfterEach {

  override def afterEach(): Unit = {
    TaskContext.unset()
  }

  test("checkInterrupted only invokes killTaskIfInterrupted at check intervals") {
    val context = newTaskContext()
    TaskContext.setTaskContext(context)

    // rowsWritten=0 -> checks (0 % 100 == 0)
    // rowsWritten=1..99 -> skips
    // rowsWritten=100 -> checks (100 % 100 == 0)
    // rowsWritten=101..199 -> skips
    for (rowsWritten <- 0L until 200L) {
      OdpsTableDataWriter.checkInterrupted(rowsWritten)
    }

    // If the interval logic were broken (e.g. always checks), a killed task
    // would throw at any row. Verify the interval gate works by killing
    // mid-interval — the next non-interval rows should NOT throw.
    context.markInterrupted("another attempt succeeded")

    // rowsWritten=150 is not a multiple of 100, so checkInterrupted should skip
    OdpsTableDataWriter.checkInterrupted(150)

    // rowsWritten=200 is a multiple of 100, so checkInterrupted should throw
    val ex = intercept[TaskKilledException] {
      OdpsTableDataWriter.checkInterrupted(200)
    }
    assert(ex.reason == "another attempt succeeded")
  }

  test("checkInterrupted throws TaskKilledException at next check interval after kill") {
    val context = newTaskContext()
    TaskContext.setTaskContext(context)

    for (rowsWritten <- 0L until 50L) {
      OdpsTableDataWriter.checkInterrupted(rowsWritten)
    }

    context.markInterrupted("another attempt succeeded")

    val ex = intercept[TaskKilledException] {
      for (rowsWritten <- 50L until 150L) {
        OdpsTableDataWriter.checkInterrupted(rowsWritten)
      }
    }
    assert(ex.reason == "another attempt succeeded")
  }

  test("checkInterrupted throws immediately when killed at interval boundary") {
    val context = newTaskContext()
    TaskContext.setTaskContext(context)

    context.markInterrupted("another attempt succeeded")

    val ex = intercept[TaskKilledException] {
      OdpsTableDataWriter.checkInterrupted(0)
    }
    assert(ex.reason == "another attempt succeeded")
  }

  test("checkKilled throws TaskKilledException immediately when task is killed") {
    val context = newTaskContext()
    TaskContext.setTaskContext(context)

    context.markInterrupted("another attempt succeeded")

    val ex = intercept[TaskKilledException] {
      OdpsTableDataWriter.checkKilled()
    }
    assert(ex.reason == "another attempt succeeded")
  }

  test("checkKilled does not throw when task is not killed") {
    TaskContext.setTaskContext(newTaskContext())

    OdpsTableDataWriter.checkKilled()
  }

  test("KILL_TASK_CHECK_INTERVAL is 100") {
    assert(OdpsTableDataWriter.KILL_TASK_CHECK_INTERVAL == 100)
  }

  private def newTaskContext(): TaskContextImpl = {
    new TaskContextImpl(
      stageId = 0,
      stageAttemptNumber = 0,
      partitionId = 0,
      taskAttemptId = 0,
      attemptNumber = 0,
      taskMemoryManager = null,
      localProperties = null,
      metricsSystem = null,
      cpus = 1)
  }
}
