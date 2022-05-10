package org.shellinterview

import org.apache.spark.sql.ForeachWriter

import org.shellinterview.StreamingApp.Emp

class CustomForeachWriter extends ForeachWriter[Emp] {
  override def open(partitionId: Long, epochId: Long): Boolean = ???

  override def process(value: Emp): Unit = ???

  override def close(errorOrNull: Throwable): Unit = ???
}
