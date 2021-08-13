package org.apache.spark.sql.execution.datasources.rest

import org.apache.spark.sql.SparkSession
import org.junit.Test

/**
  * Created by zx on 2019/11/9 - 15:03.
  */
@Test
class TestDataSourceRest {

  val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("datasource-rest")
    .config("spark.some.config.option", "some-value")
    .config("spark.driver.allowMultipleContexts", "true")
    .getOrCreate();

  @Test
  def testGet: Unit = {
    val df = spark.read.option("url","https://blog.csdn.net/qq_36330643/article/details/76947658").format("org.apache.spark.sql.execution.datasources.rest.RestDataSource").load()
    df.printSchema()
    df.show(false)
  }
}
