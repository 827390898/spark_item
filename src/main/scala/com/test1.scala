package com

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object test1 {
  /*
  json数据归纳格式（考虑获取到数据的成功因素 status=1成功 starts=0 失败）：
   1、按照pois，分类businessarea，并统计每个businessarea的总数。
   2、按照pois，分类type，为每一个Type类型打上标签，统计各标签的数量
   */
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\HADOOP\\hadoop-common-2.2.0-bin-master")
//    if(args.length != 1){
//      println("输入目录不正确")
//      sys.exit()
//    }
//    val Array(inputPath) =args

    val spark = SparkSession
      .builder()
      .appName("ct")
      .master("local")
      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    // 获取数据
    val df: DataFrame = spark.read.json("dir/json.json")
    df.printSchema()
    df.createOrReplaceTempView("tmp")
    val pois: DataFrame = spark.sql("select regeocode.pois from tmp")
    //pois.rdd.foreach(println)
    pois.createOrReplaceTempView("tmp2")
    val businessarea = spark.sql("select pois.businessarea from tmp2")
    /*
    +--------------------+
    |        businessarea|
    +--------------------+
    |[白杨, 白杨, 白杨, 白杨, ...|
    |[下沙, 下沙, 下沙, 下沙, ...|
    |[九堡街道, 九堡街道, 九堡街道...|
    |[洋泾, 洋泾, 洋泾, 洋泾, ...|
    |[新天地(自忠路), 新天地(自忠...|
    |[太平桥, 太平桥, 太平桥, 太...|
    |[城北, 城北, 城北, 城北, ...|
    |[[], [], [], [], ...|
    |[[], [], [], [], ...|
    |[[], [], [], [], ...|
    |[站北, 站北, 站北, 站北, ...|
     */
    //df2.write.partitionBy("provincename","cityname").json("C:\\shengfen")
//    businessarea.write.json("C:\\bus")
//    val res1 = businessarea.flatMap(x => (x,1))
//      .reduceByKey(_+_)
//      .collect().toBuffer.foreach(println)

  }
}
