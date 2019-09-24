package com.Location

import com.util.Reptutils
import org.apache.spark.sql.{DataFrame, SparkSession}

object Location {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\HADOOP\\hadoop-common-2.2.0-bin-master")
    if(args.length != 2){
      println("输入目录不正确")
      sys.exit()
    }
    val Array(inputPath,outputpath) =args

    val spark = SparkSession
      .builder()
      .appName("Location")
      .master("local")
      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
//  获取数据
    val df: DataFrame = spark.read.parquet(inputPath)
    df.rdd.map(row => {
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
      //返回
      ((row.getAs[String]("provincename"),row.getAs[String]("cityname")),allList)
    }).reduceByKey((list1,list2)=>{
      //list(1,1,1,1,1,1,1).zip(list2(1,1,1,1,1,1,1))=list(1,1)(1,1)(1,1)(1,1)(1,1)(1,1)(1,1)
      val tuples: List[(Double, Double)] = list1.zip(list2)
      tuples.map(t =>t._1+t._2)
    })
      .map(t => t._1+","+t._2.mkString(","))
      .saveAsTextFile(outputpath)

  }

}
