package com.Tags

import com.util.Tag
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

object TagsLocation extends Tag{
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()
    val row: Row = args(0).asInstanceOf[Row]
    //获取地域数据
    val pro: String = row.getAs[String]("provincename")
    val city: String = row.getAs[String]("cityname")
    if (StringUtils.isNotBlank(pro)){
      list:+=("ZP"+pro,1)
    }
    if (StringUtils.isNotBlank(city)){
      list:+=("ZC"+city,1)
    }

    list
  }
}
