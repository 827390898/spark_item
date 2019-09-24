package com.Tags

import com.util.TagUtils
import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.sql.{DataFrame, SparkSession}

object TagsContext2 {
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

    val allUserId = df.rdd.map(row => {
      //获取所有ID
      val strList = TagUtils.getallUserId(row)
      (strList,row)
    })
    //构建点集合
    val verties = allUserId.flatMap(row=> {
      val rows = row._2

      val adList: List[(String, Int)] = TagsAd.makeTags(rows)
      //商圈
      val businessList: List[(String, Int)] = BusinessTag.makeTags(rows)
      //媒体标签
      val appList = TagsAPP.makeTags(rows,broadValue)
      //设备标签
      val devList = TagsDevice.makeTags(rows)
      //地域标签
      val locList = TagsLocation.makeTags(rows)
      //关键字标签
      val kwList = TagsKword.makeTags(rows,broadValues)
      val tagList: List[(String, Int)] = adList++businessList++appList++devList++locList++kwList
      //保留用户ID
      val VD = row._1.map((_,0))++tagList
      //1.如何保证其中一个ID携带着用户的标签
      //2.用户ID的字符串如何处理
       row._1.map(uId=>{
         if (row._1.head.equals(uId)){
           (uId.hashCode.toLong,VD)
         }else{
           (uId.hashCode.toLong,List.empty)
         }
       })
    })
    //打印
    //verties.take(20).foreach(println)
    //构建边集合
    val edges = allUserId.flatMap(row=>{
      row._1.map(uId=>Edge(row._1.head.hashCode.toLong,uId.hashCode.toLong,0))
    })
    //edges.foreach(println)
    //构建图
    val graph = Graph(verties,edges)
    //根据图计算中的连通图算法，通过图中的分支，连通所有的点
    //然后再根据所有点，找到内部最小的点，为当前的公共点
    val vertices: VertexRDD[VertexId] = graph.connectedComponents().vertices
    //聚合所有的标签
    vertices.join(verties).map{
      case (uid,(cnId,tagsAndUserId))=>{
        (cnId,tagsAndUserId)
      }
    }.reduceByKey((list1,list2)=>{
      (list1++list2)
        .groupBy(_._1)
        .mapValues(_.map(_._2).sum)
        .toList
    }).foreach(println)
    spark.stop()

//    df.map(row =>{
//      //获取用户的唯一ID
//      val userId: List[String] = TagUtils.getallUserId(row)
//      //接下来打标签,实现
//      val adList: List[(String, Int)] = TagsAd.makeTags(row)
//      //商圈
//      val businessList: List[(String, Int)] = BusinessTag.makeTags(row)
//      //媒体标签
//      val appList = TagsAPP.makeTags(row,broadValue)
//      //设备标签
//      val devList = TagsDevice.makeTags(row)
//      //地域标签
//      val locList = TagsLocation.makeTags(row)
//      //关键字标签
//      val kwList = TagsKword.makeTags(row,broadValues)
//      (userId,adList++appList++businessList++devList++locList++kwList)
//
//       })
      // .rdd.reduceByKey((list1,list2)=>{
//      (list1:::list2).groupBy(_._1)
//        .mapValues(_.foldLeft[Int](0)(_+_._2))
//        .toList
//    }).foreach(println)

  }
}
