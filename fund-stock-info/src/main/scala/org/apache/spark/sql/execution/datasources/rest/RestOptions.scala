package org.apache.spark.sql.execution.datasources.rest

import java.util.{Locale, Properties}

import com.fasterxml.jackson.databind.ObjectMapper

/**
  * Created by zx on 2019/11/9 - 10:04.
  */
class RestOptions(@transient private val parameters: Map[String, String]) extends Serializable {

  import RestOptions._

  def this(url: String, parameters: Map[String, String]) {
    this(parameters ++ Map(RestOptions.REST_URL -> url))
  }

  def this(url: String, requestType: String, parameters: Map[String, String]) = {
    this(parameters ++ Map(
      RestOptions.REST_URL -> url,
      RestOptions.REST_REQUEST_TYPE -> requestType))
  }

  val asProperties: Properties = {
    val properties = new Properties()
    parameters.foreach { case (k, v) => properties.setProperty(k, v) }
    properties
  }

  /*
     Required Parameters
  */
  require(parameters.isDefinedAt(REST_URL), s"Option '$REST_URL' is required.")
  val url = parameters(REST_URL)

  /*
     Optional Parameters
  */
  val requestType = parameters.getOrElse(REST_REQUEST_TYPE, "GET")
  val params = parameters.getOrElse(REST_PARAMS, "")
  val headers = parameters.getOrElse(REST_HEADERS, "")
  val bodyValue = parameters.getOrElse(REST_BODY_VALUE, "")
  val responseType = parameters.getOrElse(REST_RESPONSE_TYPE, "")
  val authUsername = parameters.getOrElse(REST_BASIC_AUTH_USERNAME, "")
  val authPassword = parameters.getOrElse(REST_BASIC_AUTH_PASSWORD, "")
  val connectionTimeout = parameters.getOrElse(REST_CONNECTION_TIMEOUT, 5000).toString.toInt
  val requestTimeout = parameters.getOrElse(REST_REQUEST_TIMEOUT, 3000).toString.toInt

  def getUrlWithParameters = {
    var result: String = url;
    if (params != null && !params.isEmpty) {
      result = s"$result$params"
    }
    result
  }

  def headersToMap = {
    val mapper = new ObjectMapper();
    if (headers != null && !headers.isEmpty) mapper.readValue(headers, classOf[java.util.Map[String, Object]])
    else null
  }
}

object RestOptions {

  private val restOptionNames = collection.mutable.Set[String]()

  private def newOption(name: String): String = {
    restOptionNames += name.toLowerCase(Locale.ROOT)
    name
  }

  val REST_URL = newOption("url")
  val REST_REQUEST_TYPE = newOption("requestType")
  //JSON STRING
  val REST_PARAMS = newOption("params")
  val REST_HEADERS = newOption("headers")
  //pass json to web-server,current support json
  val REST_BODY_VALUE = newOption("body_value")
  //return reponse as josn/xml/text....
  val REST_RESPONSE_TYPE = newOption("response_type")
  //basic auth
  val REST_BASIC_AUTH_USERNAME = newOption("auth_username")
  val REST_BASIC_AUTH_PASSWORD = newOption("auth_password")
  val REST_CONNECTION_TIMEOUT = newOption("connection_timeout")
  val REST_REQUEST_TIMEOUT = newOption("request_timeout")

}
