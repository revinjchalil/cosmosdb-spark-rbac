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
package com.microsoft.azure.cosmosdb.spark.query

import com.microsoft.azure.cosmosdb.spark.config.Config
import org.apache.spark.sql.sources._

object FilterSection {

  implicit def srcFilArr2filSel(sFilters: Array[Filter])(implicit config: Config): FilterSection =
    SourceFilters(sFilters)

  def apply(sFilters: Array[Filter])(implicit config: Config): FilterSection =
    srcFilArr2filSel(sFilters)

  def apply(): FilterSection = NoFilters
}

/**
  * Trait to be implemented to those classes describing the Filter section of a CosmosDB query.
  */
trait FilterSection {
  def filtersToDBObject(): Unit
}

case object NoFilters extends FilterSection {
  override def filtersToDBObject(): Unit = {}
}

case class SourceFilters(
                          sFilters: Array[Filter],
                          parentFilterIsNot: Boolean = false
                        )(implicit config: Config) extends FilterSection {

  override def filtersToDBObject: Unit = {}
}
