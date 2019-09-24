package com.util

import org.apache.http.client.methods.{CloseableHttpResponse, HttpGet}
import org.apache.http.impl.client.{CloseableHttpClient, HttpClients}
import org.apache.http.util.EntityUtils

/*
  Http请求协议  GET请求
 */
object HttpUtil {
  /**
    *
    * @param url
    * @return json
    */
  def get(url:String):String={
    val client: CloseableHttpClient = HttpClients.createDefault()
    val httpGet = new HttpGet(url)
    //发送请求
    val httpResponse: CloseableHttpResponse = client.execute(httpGet)
    //处理返回请求结果
    EntityUtils.toString(httpResponse.getEntity,"UTF-8")


  }

}
