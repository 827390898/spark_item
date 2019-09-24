package com.diyufenbu

import org.apache.spark.sql.{DataFrame, SparkSession}

object Regional_distribution {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\HADOOP\\hadoop-common-2.2.0-bin-master")
    if(args.length != 1){
      println("输入目录不正确")
      sys.exit()
      val Array(inputPath) =args

      val spark = SparkSession
        .builder()
        .appName("ct")
        .master("local")
        .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
        .getOrCreate()
      val df: DataFrame = spark.read.parquet(inputPath)
      // 注册临时视图
      df.createTempView("log")
      spark.sql("select ispname,")
    }
  }

}
