package com.lijuntao.flink

import org.apache.flink.api.scala.ExecutionEnvironment

object BathWordCountScala {
  def main(args: Array[String]): Unit = {
    val inputPath ="D:\\data\\file"
    val outPath = "D:\\data\\result2"

    val env = ExecutionEnvironment.getExecutionEnvironment
    val text = env.readTextFile(inputPath)
    import org.apache.flink.api.scala._
    val counts = text.flatMap(_.toLowerCase.split("\\w+")).filter(_.nonEmpty).map(w =>(w,1)).groupBy(0).sum(1)
    counts.writeAsCsv(outPath,"\n"," ").setParallelism(1)
    env.execute("bathWordCountScala job")

  }
}
