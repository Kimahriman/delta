/*
 * Copyright (2021) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.delta.stats

import java.io.File

import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.test.ScanReportHelper

// scalastyle:off import.ordering.noEmptyLine
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.execution.LocalTableScanExec
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.Utils

trait AggregatePushDownSuiteBase extends QueryTest
    with SharedSparkSession
    with DeltaSQLCommandTest
    with ScanReportHelper {

  import testImplicits._

  protected override def sparkConf: SparkConf =
    super.sparkConf
      .set(DeltaSQLConf.V2_READER_ENABLED.key, "true")

  var tempDir: File = _

  def tempPath: String = tempDir.getCanonicalPath()

  val data = Seq(
    (0, "a", 1, "xyx"),
    (0, "b", 2, "zyz"),
    (0, "c", 3, null),
    (0, null, 4, "rsr"),
    (1, "a", 5, "bab"),
    (1, "b", 6, "def"),
    (1, "c", 7, null),
    (1, null, 8, "ghi"))

  def writeData(): Unit

  def fullStats: Boolean

  override def beforeAll(): Unit = {
    super.beforeAll()
    tempDir = Utils.createTempDir()
    writeData()
  }

  override def afterAll(): Unit = {
    Utils.deleteRecursively(tempDir)
    super.afterAll()
  }

  lazy val df = spark.read.format("delta").load(tempPath)

  def checkPushedLogAggregations(
    name: String,
    aggs: Seq[Column],
    filter: String = "true",
    groupBy: Seq[String] = Seq.empty,
    result: Seq[Row] = Seq.empty,
    outputFields: Seq[String] = Seq.empty
  ): Unit = {
    test(s"Aggregates are pushed down to Delta Log - $name") {
      val plans = DeltaTestUtils.withPhysicalPlansCaptured(spark) {
        checkAnswer(
          df.filter(filter).groupBy(groupBy.map(col): _*).agg(aggs.head, aggs.tail: _*),
          result
        )
      }
      val scans = plans.flatMap(_.collect {
        case s: LocalTableScanExec => s
      })
      if (fullStats) {
        assert(scans.length == 1)
        assert(scans.head.output.length == outputFields.length)
        scans.head.output.zip(outputFields).foreach { case (attr, name) =>
          assert(attr.name == name)
        }
      } else {
        assert(scans.length == 0)
      }
    }
  }

  checkPushedLogAggregations(
    "No filter or group by",
    Seq(count($"*")),
    result = Seq(Row(8)),
    outputFields = Seq("COUNT(*)"))

  checkPushedLogAggregations(
    "Group on a single partition",
    Seq(count($"*")),
    groupBy = Seq("part1"),
    result = Seq(Row(0, 4), Row(1, 4)),
    outputFields = Seq("part1", "COUNT(*)"))

  checkPushedLogAggregations(
    "Group on a multiple partitions",
    Seq(count($"*")),
    groupBy = Seq("part1", "part2"),
    result = Seq(Row(0, "a", 1), Row(0, "b", 1), Row(0, "c", 1), Row(0, null, 1), Row(1, "a", 1),
      Row(1, "b", 1), Row(1, "c", 1), Row(1, null, 1)),
    outputFields = Seq("part1", "part2", "COUNT(*)"))

  checkPushedLogAggregations(
    "Filter on partition",
    Seq(count($"*")),
    filter = "part1 = 1",
    result = Seq(Row(4)),
    outputFields = Seq("COUNT(*)"))

  checkPushedLogAggregations(
    "Filter and group on partition",
    Seq(count($"*")),
    filter = "part1 = 1",
    groupBy = Seq("part2"),
    result = Seq(Row("a", 1), Row("b", 1), Row("c", 1), Row(null, 1)),
    outputFields = Seq("part2", "COUNT(*)"))

  checkPushedLogAggregations(
    "count field without nulls",
    Seq(count($"num")),
    result = Seq(Row(8)),
    outputFields = Seq("COUNT(num)"))

  checkPushedLogAggregations(
    "count field with nulls",
    Seq(count($"str")),
    result = Seq(Row(6)),
    outputFields = Seq("COUNT(str)"))

  checkPushedLogAggregations(
    "min of numeric field",
    Seq(min("num")),
    result = Seq(Row(1)),
    outputFields = Seq("MIN(num)"))

  checkPushedLogAggregations(
    "max of numeric field",
    Seq(max("num")),
    result = Seq(Row(8)),
    outputFields = Seq("MAX(num)"))
}

class AggregatePushDownFullStatsSuite extends AggregatePushDownSuiteBase {

  import testImplicits._

  override def fullStats: Boolean = true

  override def writeData(): Unit = {
    data.toDF("part1", "part2", "num", "str")
      .repartition($"part1", $"part2")
      .write
      .format("delta")
      .partitionBy("part1", "part2")
      .mode("append")
      .save(tempPath)
  }
}

class AggregatePushDownNoStatsSuite extends AggregatePushDownSuiteBase {

  import testImplicits._

  override def fullStats: Boolean = false

  protected override def sparkConf: SparkConf =
    super.sparkConf
      .set(DeltaSQLConf.DELTA_COLLECT_STATS.key, "false")

  override def writeData(): Unit = {
    data.toDF("part1", "part2", "num", "str")
      .write
      .format("delta")
      .partitionBy("part1", "part2")
      .mode("append")
      .save(tempPath)
  }
}
