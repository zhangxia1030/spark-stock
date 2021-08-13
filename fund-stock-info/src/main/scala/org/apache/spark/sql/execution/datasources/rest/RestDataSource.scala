package org.apache.spark.sql.execution.datasources.rest

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister, RelationProvider}

/**
  * Created by zx on 2019/11/9 - 9:49.
  */
class RestDataSource extends RelationProvider with DataSourceRegister {

  override def shortName(): String = "rest"

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    val restOptions = new RestOptions(parameters)

    RestBaseRealtion(restOptions)(sqlContext.sparkSession)
  }

}
