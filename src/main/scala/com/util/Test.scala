package com.util

import com.Tags.{BusinessTag, TagsAd}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Test {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\HADOOP\\hadoop-common-2.2.0-bin-master")

    val spark = SparkSession.builder().appName("Tags")
      .master("local")
      .getOrCreate()
    import spark.implicits._

    //读取数据文件
    val df: DataFrame = spark.read.parquet("C:\\gp1923ff")

   // val arr = Array("https://restapi.amap.com/v3/geocode/regeo?&location=116.310003,39.991957&key=ba417e5d40f61654743ffe3f978beb76&radius=1000&extensions=all")

    df.map(row=>{
      // 圈
      AmapUtil.getBusinessFromAmap(
        String2Type.toDouble(row.getAs[String]("long")),
        String2Type.toDouble(row.getAs[String]("lat")))
    }).rdd.foreach(println)
  }
}
