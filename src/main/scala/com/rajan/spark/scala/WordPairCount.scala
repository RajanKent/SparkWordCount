package com.rajan.spark.scala

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.mllib.rdd.RDDFunctions.fromRDD
import org.apache.spark.{SparkConf, SparkContext}

object WordPairCount {
  def main(args: Array[String]) {

    println("Staring - Word Pair Count!")

    val inputFile = args(0)
    val outputDirs = args(1)

    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("Word Count Demo")
      .setSparkHome(System.getenv("SPARK_HOME"))

    val sc = new SparkContext(conf)
    val outPutPath = new Path(outputDirs)

    val fs = FileSystem.get(sc.hadoopConfiguration)

    if (fs.exists(outPutPath)) {
      println(s" **** Deleting old output (if any), $outputDirs:")
      fs.delete(outPutPath, true)
    }

    val input = sc.textFile(inputFile)
    val wordPairCountRDD = input.flatMap(_.split("""[\s,.;:!?]+"""))
      .map(
        _.replaceAll(
          "([',_;?!,:]|\\b(-{1,2})|(s)|(ly)|(ed)|(ing)|(ness))$|^['\"(_]", ""
        ).trim.toLowerCase
      )
      .filter(_.length > 0)
      .map( _.toLowerCase )
      .sliding(2)
      .map{ case Array(x, y) => ((x, y), 1) }
      .reduceByKey( _ + _ )
      .sortBy( z => (z._2, z._1._1, z._1._2), ascending = false )

    wordPairCountRDD.saveAsTextFile(outputDirs)

    println("Word Pair Count - Completed!")

  }
}
