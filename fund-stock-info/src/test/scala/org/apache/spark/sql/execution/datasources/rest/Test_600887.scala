package org.apache.spark.sql.execution.datasources.rest

import org.apache.spark.sql.SparkSession
import org.junit.Test

/**
 * Created by zx on 2019/11/9 - 15:03.
 */
@Test
class Test_600887 {

  val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("datasource-rest")
    .config("spark.some.config.option", "some-value")
    .config("spark.driver.allowMultipleContexts", "true")
    .getOrCreate();

  spark.sparkContext.setLogLevel("warn")

  @Test
  def test_viewInfo(): Unit = {
    val gpDF = spark.read.parquet("/Users/xia/tmp/parquet/JJ/JIGP")
    val jjDF = spark.read.parquet("/Users/xia/tmp/parquet/JJ/TTJJ-N")
    jjDF.createOrReplaceTempView("jjt")
    gpDF.createOrReplaceTempView("gpt")
    //select t.id as '基金编号', t.chineseName as '基金名称',t.code as '股票代码',t.dataText as '截止日期', t.name as '股票名称',t.ownCount as '持股数（万股）',t.ownValue as '持仓市值（万元）',t.percent as '占净值比例'
    val showF = spark.sql(
      s"""
         |
         |select g.*,t.id,t.chineseName from jjt t
         |inner join gpt g on t.id = g.jjId
         |where g.name='伊利股份'
         |""".stripMargin)

    showF.show(false)
    println(s"持有机构共:${showF.count()}家")
  }

  @Test
  def test_StatInfo(): Unit = {
    val gpDF = spark.read.parquet("/Users/xia/tmp/parquet/JJ/JIGP")
    val jjDF = spark.read.parquet("/Users/xia/tmp/parquet/JJ/TTJJ-N")
    //gpDF.withColumn("n_ownValue",replace())
    jjDF.createOrReplaceTempView("jjt")
    gpDF.createOrReplaceTempView("gpt")
    //统计
    val showF = spark.sql(
      s"""
         |select avg(w.price),max(price),min(price),sum(n_ownValue),sum(n_ownCount) from (
         |select h.*,h.n_ownValue/h.n_ownCount price from (
         |select t.id,t.chineseName,g.*,cast(regexp_replace(g.ownValue,',','') as double) n_ownValue,cast(regexp_replace(g.ownCount,',','') as double) n_ownCount from jjt t
         |inner join gpt g on t.id = g.jjId
         |where g.name='伊利股份'
         |)h
         |)w
         |""".stripMargin)

    showF.show()
    showF.printSchema()
    println(showF.count())
  }

  @Test
  def test_OtherInfo(): Unit = {
    val gpDF = spark.read.parquet("/Users/xia/tmp/parquet/JJ/JIGP")
    val jjDF = spark.read.parquet("/Users/xia/tmp/parquet/JJ/TTJJ-N")
    //gpDF.withColumn("n_ownValue",replace())
    jjDF.createOrReplaceTempView("jjt")
    gpDF.createOrReplaceTempView("gpt")
    val showF = spark.sql(
      s"""
         |select * from (
         |select h.*,h.n_ownValue/h.n_ownCount price from (
         |select t.id,t.chineseName,g.*,cast(regexp_replace(g.ownValue,',','') as double) n_ownValue,cast(regexp_replace(g.ownCount,',','') as double) n_ownCount from jjt t
         |inner join gpt g on t.id = g.jjId
         |where g.name='伊利股份'
         |)h
         |)w where w.price <= 30
         |""".stripMargin)

    showF.show()
    showF.printSchema()
    println(showF.count())
  }

}
