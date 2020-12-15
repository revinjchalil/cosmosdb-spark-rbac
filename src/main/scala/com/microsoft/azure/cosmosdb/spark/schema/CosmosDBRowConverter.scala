/**
 * The MIT License (MIT)
 * Copyright (c) 2016 Microsoft Corporation
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package com.microsoft.azure.cosmosdb.spark.schema

import java.sql.{Date, Timestamp}
import java.util

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode}

//import com.microsoft.azure.cosmosdb.Document
import com.microsoft.azure.cosmosdb.spark.CosmosDBLoggingTrait
import com.microsoft.azure.cosmosdb.spark.config.CosmosDBConfig.DefaultPreserveNullInWrite
import com.microsoft.azure.cosmosdb.spark.config.{Config, CosmosDBConfig}
import com.microsoft.azure.documentdb._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.catalyst.expressions.UnsafeMapData
import org.apache.spark.sql.types.{DataType, DecimalType, _}
import org.json.{JSONArray, JSONObject}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.JavaConverters._
import scala.collection.immutable.HashMap
import scala.collection.mutable.ListBuffer

import com.azure.cosmos.implementation.Document

/**
 * Knows how to map from some Data Source native RDD to an {{{RDD[Row]}}}
 *
 * @tparam T Original RDD type
 */
trait RowConverter[T] {

  /**
   * Given a known schema,
   * it maps an RDD of some specified type to an {{{RDD[Row}}}
   *
   * @param schema RDD native schema
   * @param rdd    Current native RDD
   * @return A brand new RDD of Spark SQL Row type.
   */
  def asRow(schema: StructType, rdd: RDD[T]): RDD[Row]

}

case class SerializationConfig(preserveNullInWrite: Boolean)

object SerializationConfig {
  def fromConfig(config: Map[String, String]) : SerializationConfig = {
    var preserveNullInWriteBoolean = CosmosDBConfig.DefaultPreserveNullInWrite;
    val preserveNullInWriteOpt = config.get(CosmosDBConfig.PreserveNullInWrite)
    if (preserveNullInWriteOpt.isDefined) {
      preserveNullInWriteBoolean = preserveNullInWriteOpt.get.toBoolean
    }
    SerializationConfig(preserveNullInWriteBoolean)
  }
  def fromConfig(config: Config) : SerializationConfig = {
    SerializationConfig(config.getOrElse(CosmosDBConfig.PreserveNullInWrite, String.valueOf(CosmosDBConfig.DefaultPreserveNullInWrite)).toBoolean)
  }
}

class CosmosDBRowConverter(serializationConfig: SerializationConfig = SerializationConfig.fromConfig(Map[String, String]())) extends RowConverter[ObjectNode]
  with JsonSupport
  with Serializable
  with CosmosDBLoggingTrait {

  val objectMapper = new ObjectMapper();

  def asRow(schema: StructType, rdd: RDD[ObjectNode]): RDD[Row] = {
    rdd.map { record => {
      try {
        recordAsRow(documentToMap(record), schema)
      } catch {
        case e: Exception =>
          logInfo(s"record is $record")
          throw e
      }
    }
    }
  }

  def asRow(schema: StructType, array: Array[ObjectNode]): Array[Row] = {
    array.map { record =>
      recordAsRow(documentToMap(record), schema)
    }
  }

  def recordAsRow(
                   json: Map[String, AnyRef],
                   schema: StructType): Row = {

    val values: Seq[Any] = schema.fields.map {
      case StructField(name, et, _, mdata)
        if mdata.contains("idx") && mdata.contains("colname") =>
        val colName = mdata.getString("colname")
        val idx = mdata.getLong("idx").toInt
        json.get(colName).flatMap(v => Option(v)).map(toSQL(_, ArrayType(et, containsNull = true))).collect {
          case elemsList: Seq[_] if elemsList.indices contains idx => elemsList(idx)
        } orNull
      case StructField(name, dataType, _, _) =>
        json.get(name).flatMap(v => Option(v)).map(toSQL(_, dataType)).orNull
    }
    new GenericRowWithSchema(values.toArray, schema)
  }

  def toSQL(value: Any, dataType: DataType): Any = {
    Option(value).map { value =>
      (value, dataType) match {
        case (list: List[AnyRef@unchecked], ArrayType(elementType, _)) =>
          null
        case (_, struct: StructType) =>
          if(JSONObject.NULL.equals(value)) return null

          val jsonMap: Map[String, AnyRef] = value match {
            case doc: ObjectNode => documentToMap(doc)
            case hm: util.HashMap[_, _] => hm.asInstanceOf[util.HashMap[String, AnyRef]].asScala.toMap
          }
          recordAsRow(jsonMap, struct)
        case (_, map: MapType) =>
          (value match {
            case document: ObjectNode => documentToMap(document)
            case _ => value.asInstanceOf[java.util.HashMap[String, AnyRef]].asScala.toMap
          }).map(element => (toSQL(element._1, map.keyType), toSQL(element._2, map.valueType)))
        case (_, array: ArrayType) =>
          if(!JSONObject.NULL.equals(value))
            value.asInstanceOf[java.util.ArrayList[AnyRef]].asScala.map(element => toSQL(element, array.elementType)).toArray
          else
            null
        case (_, binaryType: BinaryType) =>
          value.asInstanceOf[java.util.ArrayList[Int]].asScala.map(x => x.toByte).toArray
        case _ =>
          //Assure value is mapped to schema constrained type.
          enforceCorrectType(value, dataType)
      }
    }.orNull
  }

  def rowToObjectNode(row: Row): ObjectNode = {
    val objectNode: ObjectNode = objectMapper.createObjectNode();
    row.schema.fields.zipWithIndex.foreach({
      case (field, i) => {
        objectNode.set(field.name, convertToJson(row.get(i), field.dataType, isInternalRow = false))
        objectNode
      }
    })
    objectNode
  }

  def internalRowToObjectNode(internalRow: InternalRow, schema: StructType): ObjectNode = {
    val objectNode: ObjectNode = objectMapper.createObjectNode();
    schema.fields.zipWithIndex.foreach({
      case (field, i) => {
        objectNode.set(field.name, convertToJson(internalRow.get(i, field.dataType), field.dataType, isInternalRow = true))
        objectNode
      }
    })
    objectNode
  }

  // scalastyle:off cyclomatic.complexity
  private def convertToJson(element: Any, elementType: DataType, isInternalRow: Boolean): JsonNode = {
    elementType match {
      case BinaryType => objectMapper.convertValue(element.asInstanceOf[Array[Byte]], classOf[JsonNode])
      case BooleanType => objectMapper.convertValue(element.asInstanceOf[Boolean], classOf[JsonNode])
      case DateType => objectMapper.convertValue(element.asInstanceOf[Date].getTime, classOf[JsonNode])
      case DoubleType => objectMapper.convertValue(element.asInstanceOf[Double], classOf[JsonNode])
      case IntegerType => objectMapper.convertValue(element.asInstanceOf[Int], classOf[JsonNode])
      case LongType => objectMapper.convertValue(element.asInstanceOf[Long], classOf[JsonNode])
      case FloatType => objectMapper.convertValue(element.asInstanceOf[Float], classOf[JsonNode])
      case NullType => objectMapper.nullNode()
      case DecimalType() => if (element.isInstanceOf[Decimal]) {
        objectMapper.convertValue(element.asInstanceOf[Decimal].toJavaBigDecimal, classOf[JsonNode])
      } else if (element.isInstanceOf[java.lang.Long]) {
        objectMapper.convertValue(new java.math.BigDecimal(element.asInstanceOf[java.lang.Long]), classOf[JsonNode])
      } else {
        objectMapper.convertValue(element.asInstanceOf[java.math.BigDecimal], classOf[JsonNode])
      }
      case StringType =>
        if (isInternalRow) {
          objectMapper.convertValue(element.asInstanceOf[UTF8String].toString, classOf[JsonNode])
        } else {
          objectMapper.convertValue(element.asInstanceOf[String], classOf[JsonNode])
        }
      case TimestampType => if (element.isInstanceOf[java.lang.Long]) {
        objectMapper.convertValue(element.asInstanceOf[java.lang.Long], classOf[JsonNode])
      } else {
        objectMapper.convertValue(element.asInstanceOf[Timestamp].getTime, classOf[JsonNode])
      }
      case arrayType: ArrayType => arrayTypeRouterToJsonArray(arrayType.elementType, element, isInternalRow)

      case mapType: MapType if isInternalRow =>
        mapType.keyType match {
          case StringType =>
            // convert from UnsafeMapData to scala Map
            val unsafeMap = element.asInstanceOf[UnsafeMapData]
            val keys: Array[String] = unsafeMap.keyArray().toArray[UTF8String](StringType).map(_.toString)
            val values: Array[AnyRef] = unsafeMap.valueArray().toObjectArray(mapType.valueType)
            mapTypeToObjectNode(mapType.valueType, keys.zip(values).toMap, isInternalRow)
          case _ => throw new Exception(
            s"Cannot cast $element into a Json value. MapTypes must have keys of StringType for conversion into a Document"
          )
        }

      case structType: StructType => rowTypeRouterToJsonArray(element, structType)
      case _ =>
        throw new Exception(s"Cannot cast $element into a Json value. $elementType has no matching Json value.")
    }
  }
  // scalastyle:off cyclomatic.complexity

  private def mapTypeToObjectNode(valueType: DataType, data: Map[String, Any], isInternalRow: Boolean): ObjectNode = {
    val jsonObject: ObjectNode = objectMapper.createObjectNode();
    valueType match {
      case subDocuments: StructType => data.map(kv => jsonObject.put(kv._1, rowTypeRouterToJsonArray(kv._2, subDocuments)))
      case subArray: ArrayType => data.map(kv => jsonObject.put(kv._1, arrayTypeRouterToJsonArray(subArray.elementType, kv._2, isInternalRow)))
      case _ => data.map(kv => jsonObject.put(kv._1, convertToJson(kv._2, valueType, isInternalRow)))
    }
    jsonObject
  }

  private def arrayTypeRouterToJsonArray(elementType: DataType, data: Any, isInternalRow: Boolean): ArrayNode = {
    data match {
      case d: Seq[_] => arrayTypeToJsonArray(elementType, d, isInternalRow)
      case d: ArrayData => arrayDataTypeToJsonArray(elementType, d, isInternalRow)
      case _ => throw new Exception(s"Cannot cast $data into a Json value. ArrayType $elementType has no matching Json value.")
    }
  }

  private def arrayTypeToJsonArray(elementType: DataType, data: Seq[Any], isInternalRow: Boolean): ArrayNode = {
    val arrayNode = objectMapper.createArrayNode()

    elementType match {
      case subDocuments: StructType => data.foreach(x => arrayNode.add(rowTypeRouterToJsonArray(x, subDocuments)))
      case subArray: ArrayType => data.foreach(x => arrayNode.add(arrayTypeRouterToJsonArray(subArray.elementType, x, isInternalRow)))
      case _ => data.foreach(x => arrayNode.add(convertToJson(x, elementType, isInternalRow)))
    }
    // When constructing the JSONArray, the internalData should contain JSON-compatible objects in order for the schema to be mantained.
    // Otherwise, the data will be converted into String.
    arrayNode
  }

  private def arrayDataTypeToJsonArray(elementType: DataType, data: ArrayData, isInternalRow: Boolean): ArrayNode = {
    val arrayNode = objectMapper.createArrayNode()

    elementType match {
      case subDocuments: StructType => data.foreach(elementType, (_, x) => arrayNode.add(rowTypeRouterToJsonArray(x, subDocuments)))
      case subArray: ArrayType => data.foreach(elementType, (_, x) => arrayNode.add(arrayTypeRouterToJsonArray(subArray.elementType, x, isInternalRow)))
      case _ => data.foreach(elementType, (_, x) => arrayNode.add(convertToJson(x, elementType, isInternalRow)))
    }
    // When constructing the JSONArray, the internalData should contain JSON-compatible objects in order for the schema to be mantained.
    // Otherwise, the data will be converted into String.
    arrayNode
  }

  private def rowTypeRouterToJsonArray(element: Any, schema: StructType) : ObjectNode = {
    element match {
      case e: Row => rowToObjectNode(e)
      case e: InternalRow => internalRowToObjectNode(e, schema)
      case _ => throw new Exception(s"Cannot cast $element into a Json value. Struct $element has no matching Json value.")
    }
  }

  def documentToMap(document: ObjectNode): Map[String, AnyRef] = {
    if (document == null) {
      new HashMap[String, AnyRef]
    } else {
      getMap(document).asScala.toMap
    }
  }

  def getMap(objectNode: ObjectNode): java.util.HashMap[String, AnyRef] = {
    objectMapper.convertValue(objectNode, classOf[java.util.HashMap[String, AnyRef]])
  }

  def rowToJSONObject(row: Row): JSONObject = {
    val jsonObject: JSONObject = new JSONObject()
    if (serializationConfig.preserveNullInWrite) {
      row.schema.fields.zipWithIndex.foreach({
        case (field, i) => jsonObject.put(field.name, convertToJson(row.get(i), field.dataType, isInternalRow = false))
      })
    } else {
      row.schema.fields.zipWithIndex.foreach({
        case (field, i) if row.isNullAt(i) => if (field.dataType == NullType) jsonObject.remove(field.name)
        case (field, i) => jsonObject.put(field.name, convertToJson(row.get(i), field.dataType, isInternalRow = false))
      })
    }

    jsonObject
  }

  def internalRowToJSONObject(internalRow: InternalRow, schema: StructType): JSONObject = {
    val jsonObject: JSONObject = new JSONObject()
    if (serializationConfig.preserveNullInWrite) {
      schema.fields.zipWithIndex.foreach({
        case (field, i) => jsonObject.put(field.name, convertToJson(internalRow.get(i, field.dataType), field.dataType, isInternalRow = true))
      })
    } else {
      schema.fields.zipWithIndex.foreach({
        case (field, i) if internalRow.isNullAt(i) => if (field.dataType == NullType) jsonObject.remove(field.name)
        case (field, i) => jsonObject.put(field.name, convertToJson(internalRow.get(i, field.dataType), field.dataType, isInternalRow = true))
      })
    }
    jsonObject
  }

}
