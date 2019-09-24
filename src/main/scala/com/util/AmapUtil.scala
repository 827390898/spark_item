package com.util

import java.lang

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import netscape.javascript.JSObject

import scala.collection.mutable.ListBuffer

/*
  从高德地图获取商圈信息
 */
object AmapUtil {
  def getBusinessFromAmap(long: Double,lat:Double):String={
    //https://restapi.amap.com/v3/geocode/regeo?&location=116.310003,39.991957&key=ba417e5d40f61654743ffe3f978beb76
    // &radius=1000&extensions=all
    val location = long+","+lat
    val url ="https://restapi.amap.com/v3/geocode/regeo?&location="+location+"&key=ba417e5d40f61654743ffe3f978beb76&extensions=all"
    //调用接口Http接口发送请求
    val jsonstr = HttpUtil.get(url)
    //解析json串
    val jSONObject1: JSONObject = JSON.parseObject(jsonstr)
    //判断当前状态是否为1
    val status: Int = jSONObject1.getIntValue("status")
    if (status == 0) return ""
      //如果不为空
    val jSONObject: JSONObject = jSONObject1.getJSONObject("regeocode")
    if (jSONObject == null) return ""
    val jsonObject2: JSONObject = jSONObject.getJSONObject("addressComponent")
    if (jsonObject2 == null) return ""
    val jSONArray: JSONArray = jsonObject2.getJSONArray("businessAreas")
    if (jSONArray == null) return ""

    //定义集合取值
    val result: ListBuffer[String] = collection.mutable.ListBuffer[String]()
    //循环数组（有多个）
    for (item <- jSONArray.toArray()){
      if (item.isInstanceOf[JSONObject]){
        val json = item.asInstanceOf[JSONObject]
        val name = json.getString("name")
        result.append(name)

      }

    }
    result.mkString(",")
  }

}
