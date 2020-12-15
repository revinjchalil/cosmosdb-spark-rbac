package com.microsoft.azure.cosmosdb.spark

import com.azure.cosmos.models.SqlQuerySpec

case class CosmosQuerySpec(query: String)
  extends Serializable {
  def toQuerySpec(): SqlQuerySpec = {
    new SqlQuerySpec(query)
  }
}