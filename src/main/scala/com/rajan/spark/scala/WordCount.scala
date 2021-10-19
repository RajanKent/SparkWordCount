package com.rajan.spark.scala

import org.apache.spark.sql.SparkSession

object WordCount {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .master("local[*]")
      .appName("Spark Word Count")
      .getOrCreate()

    val sc = spark.sparkContext
    val lines = sc.textFile(args(0));
    val words = lines.flatMap(_.split(" ")).map(_.replaceAll("([_;?!,:)']|(s)|(ly)|(ed)|(ing)|(ness)|\\b(-{1,2}))$|^['\"(_]", "").trim.toLowerCase).filter(_.length > 0)
    val wordsKVRdd = words.map(x => (x, 1))
    val counts = wordsKVRdd.reduceByKey((x, y) => x + y).map(x => (x._2, x._1)).sortByKey(false).map(x => (x._2, x._1))
    counts.coalesce(1).saveAsTextFile(args(1))
    spark.close()
    spark.stop()
  }
}
