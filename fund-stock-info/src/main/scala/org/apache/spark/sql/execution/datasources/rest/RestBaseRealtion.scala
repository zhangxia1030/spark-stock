package org.apache.spark.sql.execution.datasources.rest

import java.io.File
import java.util.ArrayList

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.commons.io.FileUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext, SparkSession}

import scala.util.parsing.json.{JSON, JSONArray}

/**
  * Created by zx on 2019/11/9 - 9:53.
  */
case class RestBaseRealtion(restOptions: RestOptions)(@transient val sparkSession: SparkSession)
  extends BaseRelation with TableScan {

  override def sqlContext: SQLContext = sparkSession.sqlContext

  private val restUtil = RestUtil.apply(restOptions)
  private val responseContent = {
    restOptions.requestType match {
      case "GET" => restUtil.doGet()
      case "POST" => restUtil.doGet()
      case _ => throw new NotImplementedError(s"not support type for ${restOptions.requestType}")
    }
  }

  override def schema: StructType = {
    var schema: StructType = null

    restOptions.responseType match {
      case "json" => {
        schema = sparkSession.read.json(responseContent).schema
      }
      case _ => {
        schema = StructType(
          List(
            StructField("value", StringType, false)
          )
        )
      }
    }

    schema
  }

  override def buildScan(): RDD[Row] = {

    var result :RDD[Row] = null

    restOptions.responseType match {
      case "json" => {
        val path = s"${Thread.currentThread().getContextClassLoader.getResource("/")}/${System.currentTimeMillis()}.json"
        FileUtils.write(new File(path),responseContent,false)
        result = sparkSession.read.json(path).rdd
      }
      case _ => {
        import sparkSession.implicits._
        result = sparkSession.sparkContext.parallelize(Seq(responseContent)).toDF().rdd
      }
    }

    result

  }
}
