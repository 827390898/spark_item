package com.Tags

import ch.hsr.geohash.GeoHash
import com.util.{AmapUtil, JedisConnectionPool, String2Type, Tag}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row
import redis.clients.jedis.Jedis

/*
  商圈标签
 */
object BusinessTag extends Tag{
  override def makeTags(args: Any*): List[(String, Int)] = {
    val row: Row = args(0).asInstanceOf[Row]
    var list = List[(String, Int)]()
    //获取数据
    if (String2Type.toDouble(row.getAs[String]("long")) >= 73
      && String2Type.toDouble(row.getAs[String]("long")) <= 136
      && String2Type.toDouble(row.getAs[String]("lat")) >= 3
      && String2Type.toDouble(row.getAs[String]("lat")) <= 53) {

      val long: Double = row.getAs[String]("long").toDouble
      val lat = row.getAs[String]("lat").toDouble
      //获取到商圈名称
      val business: String = getBusiness(long, lat)
      if (StringUtils.isNoneBlank(business)) {
        val str: Array[String] = business.split(",")
        str.foreach(str => {
          list :+= (str, 1)
        })
      }
    }
    list
  }
    /*
    获取商圈信息
     */
    def getBusiness(long:Double,lat:Double): String ={
      //GeoHash码
      val geohash: String = GeoHash.geoHashStringWithCharacterPrecision(lat,long,6)
      //数据库查询当前商圈消息
      var business: String = redis_queryBusiness(geohash)
      //如果没有商圈geohash编码，去高德请求
      if(business == null){
        business = AmapUtil.getBusinessFromAmap(long,lat)
        //将高德获取的商圈存储redis数据库
        if (business != null && business.length>0){
        redis_insertBusiness(geohash,business)
        }
      }
      business
    }
    def redis_queryBusiness(geoHash: String): String ={
      val jedis: Jedis = JedisConnectionPool.getConnection()
      val business: String = jedis.get(geoHash)
      jedis.close()
      business

    }

    /**
      * 将商圈保存到redis数据库
      */
    def redis_insertBusiness(geoHash: String,business:String): Unit ={
      val jedis: Jedis = JedisConnectionPool.getConnection()
      jedis.set(geoHash,business)
      jedis.close()


    }
  }


