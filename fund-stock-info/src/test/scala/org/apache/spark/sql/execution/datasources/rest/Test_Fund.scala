package org.apache.spark.sql.execution.datasources.rest

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util

import com.google.common.base.Splitter
import com.zx.test.JJUtils
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.jsoup.Jsoup
import org.junit.Test

/**
 * Created by zx on 2019/11/9 - 15:03.
 */
@Test
class Test_Fund {

  val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("datasource-rest")
    .config("spark.some.config.option", "some-value")
    .config("spark.driver.allowMultipleContexts", "true")
    .getOrCreate();

  spark.sparkContext.setLogLevel("warn")

  @Test
  def test_GetAllFund(): Unit = {
    val nurl = "http://fund.eastmoney.com/js/fundcode_search.js"
    val df = spark.read.option("url", nurl).option("requestType", "GET").format("org.apache.spark.sql.execution.datasources.rest.RestDataSource").load()
    //df.printSchema()

    //val value = Encoders.kryo(Tuple1(String).getClass)
    import spark.implicits._
    val clist = df.map(r => {
      r.getAs[String](0).replace("var r = ", "") //.replaceAll("\"", "")
    }).collectAsList

    val array = clist.get(0).split("],\\[").map(s => s.replaceAll("\\[", "").replaceAll("]", ""))
    //println(array.length)
    //println(array(0))

    val records = array.map(s => {
      val item = s.split(",")
      //http://fund.eastmoney.com/pingzhongdata/000209.js?v=20210810183213
      JJRecord(item(0).replaceAll("\"", ""), item(1).replaceAll("\"", ""), item(2).replaceAll("\"", ""), item(3).replaceAll("\"", ""), item(4).replaceAll("\"", ""))
    })

    val dfJJ = spark.createDataFrame(records)
    dfJJ.write.parquet("/Users/xia/tmp/parquet/fund")
    dfJJ.show()
    dfJJ.printSchema()
  }

  case class JJRecord(id: String, reduceName: String, chineseName: String, typeName: String, fullPYName: String)

}
