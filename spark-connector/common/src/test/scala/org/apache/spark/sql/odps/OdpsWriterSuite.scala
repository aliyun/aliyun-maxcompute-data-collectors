package org.apache.spark.sql.odps

import org.apache.spark.{TaskContext, TaskContextImpl}
import org.scalatest.funsuite.AnyFunSuite

class OdpsWriterSuite extends AnyFunSuite {

  test("test odps writer attempt number") {
    try {
      val context1 = new TaskContextImpl(0, 0, 0, 0, 0, null, null, null)
      TaskContext.setTaskContext(context1)
      val attemptNumber1 = (TaskContext.get.stageAttemptNumber << 16) | TaskContext.get.attemptNumber
      assert(attemptNumber1 == 0)

      val context2 = new TaskContextImpl(0, 1, 0, 0, 0, null, null, null)
      TaskContext.setTaskContext(context2)
      val attemptNumber2 = (TaskContext.get.stageAttemptNumber << 16) | TaskContext.get.attemptNumber
      assert(attemptNumber2 == 65536)

      val context3 = new TaskContextImpl(0, 1, 0, 0, 1, null, null, null)
      TaskContext.setTaskContext(context3)
      val attemptNumber3 = (TaskContext.get.stageAttemptNumber << 16) | TaskContext.get.attemptNumber
      assert(attemptNumber3 == 65537)

      val context4 = new TaskContextImpl(0, 2, 0, 0, 1, null, null, null)
      TaskContext.setTaskContext(context4)
      val attemptNumber4 = (TaskContext.get.stageAttemptNumber << 16) | TaskContext.get.attemptNumber
      assert(attemptNumber4 == 131073)

      val context5 = new TaskContextImpl(0, Short.MaxValue, 0, 0, 0, null, null, null)
      TaskContext.setTaskContext(context5)
      val attemptNumber5 = (TaskContext.get.stageAttemptNumber << 16) | TaskContext.get.attemptNumber
      assert(attemptNumber5 == 2147418112)

      val context6 = new TaskContextImpl(0, Short.MaxValue, 0, 0, (1 << 16) - 1, null, null, null)
      TaskContext.setTaskContext(context6)
      val attemptNumber6 = (TaskContext.get.stageAttemptNumber << 16) | TaskContext.get.attemptNumber
      // 2147483647
      assert(attemptNumber6 == Int.MaxValue)

      val context7 = new TaskContextImpl(0, 0, 0, 0, (1 << 17) - 1, null, null, null)
      TaskContext.setTaskContext(context7)
      val attemptNumber7 = (TaskContext.get.stageAttemptNumber << 16) | TaskContext.get.attemptNumber
      // (1 << 17) - 1
      assert(attemptNumber7 == 131071)

      val context8 = new TaskContextImpl(0, 1, 0, 0, (1 << 17) - 1, null, null, null)
      TaskContext.setTaskContext(context8)
      val attemptNumber8 = (TaskContext.get.stageAttemptNumber << 16) | TaskContext.get.attemptNumber
      // (1 << 17) - 1
      assert(attemptNumber8 == 131071)
    } finally {
      TaskContext.unset()
    }
  }

}

