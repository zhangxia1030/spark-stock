package org.apache.spark.sql.execution.datasources.rest

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util

import com.google.common.base.Splitter
import com.rayfay.test.JJUtils
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.jsoup.Jsoup
import org.junit.Test
import com.rayfay.test.StockInfo

/**
 * Created by zx on 2019/11/9 - 15:03.
 */
@Test
class Test_Stock {

  val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("datasource-rest")
    .config("spark.some.config.option", "some-value")
    .config("spark.driver.allowMultipleContexts", "true")
    .getOrCreate();

  spark.sparkContext.setLogLevel("warn")

  @Test
  def test_GetStockInfoByStockId(): Unit = {
    val df = spark.read.parquet("/Users/xia/tmp/parquet/JJ/TTJJ")
    //val array = df.randomSplit(Array(2, 2, 2, 2))

    var list = new util.ArrayList[StockInfo]()
    df.collect().foreach(r=>{
      Thread.sleep(235)
      val id = r.getAs[String]("id")
      var infoes = JJUtils.getStockInfoByJJId(id)
      println(s"$id>>>>$infoes")
      list.addAll(infoes)
      if(list.size() >= 5000){
        println(s"write one:${list.size()}")
        val gdf = spark.createDataFrame(list, new StockInfo().getClass)
        gdf.write.mode(SaveMode.Append).parquet("/Users/xia/tmp/parquet/JJ/JIGP")
        list = new util.ArrayList[StockInfo]()
      }
      infoes = null;
    })

    val gdf = spark.createDataFrame(list, new StockInfo().getClass)
    gdf.write.mode(SaveMode.Append).parquet("/Users/xia/tmp/parquet/JJ/JIGP")
  }
}
