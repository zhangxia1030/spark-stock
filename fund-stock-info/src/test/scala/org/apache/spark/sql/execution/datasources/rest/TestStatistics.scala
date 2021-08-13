package org.apache.spark.sql.execution.datasources.rest

import org.apache.spark.sql.SparkSession
import org.junit.Test

/**
 * Created by zx on 2019/11/9 - 15:03.
 */
@Test
class TestStatistics {

  val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("datasource-rest")
    .config("spark.some.config.option", "some-value")
    .config("spark.driver.allowMultipleContexts", "true")
    .getOrCreate();

  spark.sparkContext.setLogLevel("warn")


  @Test
  def test_SortFund(): Unit = {
    val gpDF = spark.read.parquet("/Users/xia/tmp/parquet/JJ/JIGP")
    val jjDF = spark.read.parquet("/Users/xia/tmp/parquet/JJ/TTJJ-N")
    //gpDF.withColumn("n_ownValue",replace())
    jjDF.createOrReplaceTempView("jjt")
    gpDF.createOrReplaceTempView("gpt")
    //统计
    val showF = spark.sql(
      s"""
         |select * from (
         |select w.name, avg(w.price),max(price),min(price),sum(n_ownValue) sum_n_ownValue,sum(n_ownCount),count(1) ct from (
         |select h.*,h.n_ownValue/h.n_ownCount price from (
         |select t.id,t.chineseName,g.*,cast(regexp_replace(g.ownValue,',','') as double) n_ownValue,cast(regexp_replace(g.ownCount,',','') as double) n_ownCount from jjt t
         |inner join gpt g on t.id = g.jjId
         |)h
         |)w group by w.name
         |) order by sum_n_ownValue desc
         |""".stripMargin)

    showF.show()
    showF.printSchema()
    println(showF.count())
  }
}
