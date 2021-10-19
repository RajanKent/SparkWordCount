package com.rajan.spark.scala

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

object WordCount {
  def main(args: Array[String]): Unit = {

    println("------- Staring: Word Count!----------")

    val spark = SparkSession.builder
      .master("local[*]")
      .appName("Spark Word Count")
      .getOrCreate()

    val inputFile = args(0)
    val outputDirs = args(1)

    val sc = spark.sparkContext
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val outPutPath = new Path(outputDirs)

    if (fs.exists(outPutPath)) {
      println(s" **** Deleting old output (if any), $outputDirs:")
      fs.delete(outPutPath, true)
    }

    val lines = sc.textFile(inputFile);
    val words = lines.flatMap(_.split("""[\s,.;:!?]+"""))
      .map(
        _.replaceAll(
          "([',_;?!,:]|\\b(-{1,2})|(s)|(ly)|(ed)|(ing)|(ness))$|^['\"(_]", ""
        ).trim
      )
      .filter(_.length > 0)
      .map(_.toLowerCase)

    val wordsKVRdd = words.map(x => (x, 1))
    val counts = wordsKVRdd.reduceByKey((x, y) => x + y)
      .map(x => (x._2, x._1))
      .sortByKey(false)
      .map(x => (x._2, x._1))

    counts.coalesce(1).saveAsTextFile(outputDirs)

    println("-----------Word Count - Completed!---------")

    spark.close()
    spark.stop()
  }
}
