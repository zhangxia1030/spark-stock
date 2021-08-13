package org.apache.spark.sql.execution.datasources.rest

import java.io.{IOException, InputStream}

import com.rayfay.daas.util.HttpCollectUtils
import org.apache.commons.httpclient.HttpStatus
import org.apache.commons.lang.StringUtils
import org.apache.http.HttpEntity
import org.apache.http.auth.{AuthScope, UsernamePasswordCredentials}
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.methods.{HttpGet, HttpPost}
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.{BasicCredentialsProvider, CloseableHttpClient, HttpClientBuilder}
import org.slf4j.LoggerFactory

import scala.io.Source

/**
  * Created by zx on 2019/11/9 - 11:38.
  */
class RestUtil(restOptions: RestOptions) {

  val logger = LoggerFactory.getLogger(classOf[RestUtil])

  private def buildClient: CloseableHttpClient = {
    val httpClientBuilder = HttpClientBuilder.create
    if (StringUtils.isNotBlank(restOptions.authUsername) && StringUtils.isNotBlank(restOptions.authPassword)) {
      val basicCredentialsProvider = new BasicCredentialsProvider
      val creds = new UsernamePasswordCredentials(restOptions.authUsername, restOptions.authPassword)
      basicCredentialsProvider.setCredentials(AuthScope.ANY, creds)
      httpClientBuilder.setDefaultCredentialsProvider(basicCredentialsProvider)
    }
    httpClientBuilder.build
  }

  private def close(client: CloseableHttpClient): Unit = {
    try
      client.close()
    catch {
      case e: IOException =>
        logger.error("could not close client", e)
    }
  }

  private def getContentToString(entity: HttpEntity): String = {
    var in: InputStream = null
    var result: String = null
    try {
      in = entity.getContent
      result = Source.fromInputStream(in).mkString
    } catch {
      case e: Exception =>
        logger.error("Error occurs when trying to reading response", e)
    } finally {
      if (in != null) {
        in.close
      }
    }
    result
  }

  private def ifGetWithHeaders(httpRequestBase: HttpGet) = {
    if( restOptions.headersToMap == null || restOptions.headersToMap.isEmpty) httpRequestBase
    val headers = HttpCollectUtils.buildHeaders(restOptions.headersToMap)
    if (headers != null && headers.size > 0) httpRequestBase.setHeaders(headers)
    httpRequestBase
  }

  private def ifPostWithHeaders(httpRequestBase: HttpPost) = {
    if(restOptions.headersToMap == null || restOptions.headersToMap.isEmpty) httpRequestBase
    val headers = HttpCollectUtils.buildHeaders(restOptions.headersToMap)
    if (headers != null && headers.size > 0) httpRequestBase.setHeaders(headers)
    httpRequestBase
  }

  def doGet() = {
    val httpClient = buildClient
    var result: String = null
    try {
      var httpGet = new HttpGet(restOptions.getUrlWithParameters)
      httpGet = ifGetWithHeaders(httpGet)
      val requestConfig = RequestConfig.custom()
        .setConnectTimeout(restOptions.connectionTimeout).setConnectionRequestTimeout(restOptions.requestTimeout)
        .setSocketTimeout(restOptions.connectionTimeout).build();
      httpGet.setConfig(requestConfig)
      val response = httpClient.execute(httpGet)
      val statusCode = response.getStatusLine().getStatusCode()
      if (statusCode == HttpStatus.SC_OK) {
        result = getContentToString(response.getEntity)
      }
    }

    result
  }

  def doPost() = {
    val httpClient = buildClient
    var result: String = null
    try {
      var httpPost = new HttpPost(restOptions.getUrlWithParameters)

      //pass value to web
      val requestEntity = new StringEntity(restOptions.bodyValue, "application/json", "UTF-8")
      httpPost.setEntity(requestEntity)
      httpPost = ifPostWithHeaders(httpPost)
      val response = httpClient.execute(httpPost)
      val statusCode = response.getStatusLine().getStatusCode()
      if (statusCode == HttpStatus.SC_OK) {
        result = getContentToString(response.getEntity)
      }
    }

    result
  }

}

object RestUtil {
  def apply(restOptions: RestOptions): RestUtil = new RestUtil(restOptions)
}
