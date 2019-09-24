package com.graph_Test

import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.sql.SparkSession

object GraphTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("graph").master("local[*]")
      .getOrCreate()
    //创建点和边
    //构建点的集合
    val vertexRDD = spark.sparkContext.makeRDD(Seq(
      (1L,("小憨",26)),
      (2L,("小傻",30)),
      (6L,("小品",33)),
      (9L,("小如",26)),
      (133L,("小鸡",30)),
      (138L,("小崽子",33)),
      (158L,("小杰",26)),
      (16L,("小哲",30)),
      (44L,("小强",33)),
      (21L,("小胡",26)),
      (5L,("小狗",30)),
      (7L,("小熊",33))
    ))
    val edgeRDD = spark.sparkContext.makeRDD(Seq(
      Edge(1L,133L,0),
      Edge(2L,133L,0),
      Edge(6L,133L,0),
      Edge(9L,133L,0),
      Edge(6L,138L,0),
      Edge(16L,138L,0),
      Edge(21L,138L,0),
      Edge(44L,138L,0),
      Edge(5L,158L,0),
      Edge(7L,158L,0)
    ))

    //  构件图
    val graph: Graph[(String, Int), Int] = Graph(vertexRDD,edgeRDD)
    //取顶点
    /*
    (16,1)
    (44,1)
    (7,5)
    (21,1)
    (133,1)
    (1,1)
    (9,1)
    (5,5)
    (158,5)
    (138,1)
    (6,1)
    (2,1)
     */
    val vertices = graph.connectedComponents().vertices
    //匹配数据
    vertices.join(vertexRDD).map{
      case (userId,(cnId,(name,age)))=>(cnId,List((name,age)))
    }.reduceByKey(_++_).foreach(println)

    /*
    (5,List((小熊,33)))
    (1,List((小哲,30)))
    (1,List((小强,33)))
    (1,List((小胡,26)))
    (1,List((小鸡,30)))
    (1,List((小憨,26)))
    (1,List((小如,26)))
    (5,List((小狗,30)))
    (5,List((小杰,26)))
    (1,List((小崽子,33)))
    (1,List((小品,33)))
    (1,List((小傻,30)))
     */


  }

}
