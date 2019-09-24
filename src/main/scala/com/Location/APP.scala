package com.Location

import com.util.Reptutils
import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, SparkSession}

object APP {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir","C:\\HADOOP\\hadoop-common-2.2.0-bin-master")
    if (args.length != 3){
      println("输入目录不正确")
      sys.exit()
    }
    val Array(inputPath,outputPath,docs) = args
    val spark: SparkSession = SparkSession.builder()
      .appName("APP")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    //读取数据字典
    val docMap = spark.sparkContext.textFile(docs).map(_.split("\\s",-1))
      .filter(_.length>=5).map(arr => (arr(4),arr(1))).collectAsMap()
    //进行广播
    val broadcast: Broadcast[collection.Map[String, String]] = spark.sparkContext.broadcast(docMap)
    //读取数据文件
    val df: DataFrame = spark.read.parquet(inputPath)
    df.rdd.map(row =>{
      //取媒体相关字段
      var appName = row.getAs[String]("appname")
      if(StringUtils.isBlank("appname")){
        appName = broadcast.value.getOrElse(row.getAs[String]("appid"),"unknow")
      }
      //根据指标的字段获取数据
      val requestmode: Int = row.getAs[Int]("requestmode")
      val processnode: Int = row.getAs[Int]("processnode")
      val iseffective: Int = row.getAs[Int]("iseffective")
      val isbilling: Int = row.getAs[Int]("isbilling")
      val isbid: Int = row.getAs[Int]("isbid")
      val iswin: Int = row.getAs[Int]("iswin")
      val adorderid: Int = row.getAs[Int]("adorderid")
      //竞价成功价格
      val winprice = row.getAs[Double]("winprice")
      //转换后的广告消费
      val adpayment = row.getAs[Double]("adpayment")
      //处理请求数
      val rptList = Reptutils.ReqPt(requestmode,processnode)
      //处理展示点击
      val clickList = Reptutils.ClickPt(requestmode,iseffective)
      //处理竞价总数，竞价成功数，广告消费，广告成本
      val adList = Reptutils.adPt(iseffective,isbilling,isbid,iswin,adorderid,winprice,adpayment)
      //所有List拼接成一个新的AllList
      val allList: List[Double] = rptList ++ clickList ++ adList
      (appName,allList)
    }).reduceByKey((list1,list2)=>{
      //list(1,1,1,1,1,1,1).zip(list2(1,1,1,1,1,1,1))=list(1,1)(1,1)(1,1)(1,1)(1,1)(1,1)(1,1)
      val tuples: List[(Double, Double)] = list1.zip(list2)
      tuples.map(t =>t._1+t._2)
    })
      .map(t => t._1+","+t._2.mkString(","))
      .saveAsTextFile(outputPath)

  }
}
