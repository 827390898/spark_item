package com.Tags

import com.util.TagUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object TagsContext {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\HADOOP\\hadoop-common-2.2.0-bin-master")
    if(args.length != 3){
      println("目录不正确")
      sys.exit()
    }
    val Array(inputPath,docs,stopwords) = args
    //创建spark上下文
    val spark = SparkSession.builder().appName("Tags")
      .master("local")
      .getOrCreate()
    import spark.implicits._
    //读取数据文件
    val df: DataFrame = spark.read.parquet(inputPath)
    //读取字典文件
    val docsRDD = spark.sparkContext.textFile(docs)
        .map(_.split("\\s")).filter(_.length>=5)
      .map(arr => (arr(4),arr(1))).collectAsMap()
    //广播字典
    val broadValue = spark.sparkContext.broadcast(docsRDD)
    //读取停用字典
    val stopwordsRDD = spark.sparkContext.textFile(docs)
      .map((_,0)).collectAsMap()
    //字典文件
    val broadValues = spark.sparkContext.broadcast(stopwordsRDD)

    df.map(row =>{
      //获取用户的唯一ID
      val userId = TagUtils.getallUserId(row)
      //接下来打标签,实现
      val adList: List[(String, Int)] = TagsAd.makeTags(row)
      //商圈
      val businessList: List[(String, Int)] = BusinessTag.makeTags(row)
      //媒体标签
      val appList = TagsAPP.makeTags(row,broadValue)
      //设备标签
      val devList = TagsDevice.makeTags(row)
      //地域标签
      val locList = TagsLocation.makeTags(row)
      //关键字标签
      val kwList = TagsKword.makeTags(row,broadValues)
      (userId,adList++appList++businessList++devList++locList++kwList)

    }).rdd.reduceByKey((list1,list2)=>{
      (list1:::list2).groupBy(_._1)
        .mapValues(_.foldLeft[Int](0)(_+_._2))
        .toList
    }).foreach(println)

  }
}
