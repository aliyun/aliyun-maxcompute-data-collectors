package org.apache.spark.sql.odps

import org.apache.spark.{TaskContext, TaskContextImpl, TaskKilledException}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterEach

class OdpsTableDataWriterSuite extends AnyFunSuite with BeforeAndAfterEach {

  private val KILL_TASK_CHECK_INTERVAL = 100

  override def afterEach(): Unit = {
    TaskContext.unset()
  }

  test("checkInterrupted does not throw when task is not killed") {
    TaskContext.setTaskContext(newTaskContext())

    for (rowsWritten <- 0L until 200L) {
      checkInterrupted(rowsWritten)
    }
  }

  test("checkInterrupted throws TaskKilledException at next check interval after kill") {
    val context = newTaskContext()
    TaskContext.setTaskContext(context)

    for (rowsWritten <- 0L until 50L) {
      checkInterrupted(rowsWritten)
    }

    context.markInterrupted("another attempt succeeded")

    val ex = intercept[TaskKilledException] {
      for (rowsWritten <- 50L until 150L) {
        checkInterrupted(rowsWritten)
      }
    }
    assert(ex.reason == "another attempt succeeded")
  }

  test("checkKilled throws TaskKilledException immediately when task is killed") {
    val context = newTaskContext()
    TaskContext.setTaskContext(context)

    context.markInterrupted("another attempt succeeded")

    // checkKilled checks every call, no interval
    val ex = intercept[TaskKilledException] {
      checkKilled()
    }
    assert(ex.reason == "another attempt succeeded")
  }

  test("checkKilled before sleep exits retry loop early on killed task") {
    val context = newTaskContext()
    TaskContext.setTaskContext(context)

    context.markInterrupted("another attempt succeeded")

    val ex = intercept[TaskKilledException] {
      var retries = 0
      while (retries < 3) {
        retries += 1
        checkKilled()
        Thread.sleep(100)
      }
    }
    assert(ex.reason == "another attempt succeeded")
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

  private def checkInterrupted(rowsWritten: Long): Unit = {
    if (rowsWritten % KILL_TASK_CHECK_INTERVAL == 0) {
      checkKilled()
    }
  }

  private def checkKilled(): Unit = {
    TaskContext.get().killTaskIfInterrupted()
  }
}
