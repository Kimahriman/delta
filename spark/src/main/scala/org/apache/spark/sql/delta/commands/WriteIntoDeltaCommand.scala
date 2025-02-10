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

package org.apache.spark.sql.delta.commands

import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.constraints.{Constraint, DeltaInvariantCheckerExec}
import org.apache.spark.sql.delta.actions.{Metadata, Protocol}
import org.apache.spark.sql.delta.files.DeltaFileFormatWriter
import org.apache.spark.sql.delta.perf.DeltaOptimizedWriterExec
import org.apache.spark.sql.delta.schema._
import org.apache.spark.sql.delta.sources.DeltaSQLConf

import org.apache.spark.internal.io.FileCommitProtocol
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeSet, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{ProjectExec, SparkPlan}
import org.apache.spark.sql.execution.command.DataWritingCommand
import org.apache.spark.sql.execution.datasources.{FileFormatWriter, WriteJobStatsTracker}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StringType

case class WriteIntoDeltaCommand(
  deltaLog: DeltaLog,
  query: LogicalPlan,
  outputSpec: FileFormatWriter.OutputSpec,
  writeOptions: Option[DeltaOptions],
  isOptimize: Boolean,
  protocol: Protocol,
  metadata: Metadata,
  committer: FileCommitProtocol,
  partitioningColumns: Seq[Attribute],
  deltaConstraints: Seq[Constraint],
  statsTrackers: Seq[WriteJobStatsTracker]
) extends DataWritingCommand {

  override def output: Seq[Attribute] = query.output

  def outputColumnNames: Seq[String] = output.map(_.name)

  def run(sparkSession: SparkSession, child: SparkPlan): Seq[Row] = {

    val empty2NullPlan = convertEmptyToNullIfNeeded(child,
      partitioningColumns, deltaConstraints)
    val checkInvariants = DeltaInvariantCheckerExec(empty2NullPlan, deltaConstraints)
    // No need to plan optimized write if the write command is OPTIMIZE, which aims to produce
    // evenly-balanced data files already.
    val physicalPlan = if (!isOptimize &&
      shouldOptimizeWrite(writeOptions, sparkSession.sessionState.conf)) {
      DeltaOptimizedWriterExec(checkInvariants, metadata.partitionColumns, deltaLog)
    } else {
      checkInvariants
    }

    // Iceberg spec requires partition columns in data files
    val writePartitionColumns = IcebergCompat.isAnyEnabled(metadata)
    // Retain only a minimal selection of Spark writer options to avoid any potential
    // compatibility issues
    val options = (writeOptions match {
      case None => Map.empty[String, String]
      case Some(writeOptions) =>
        writeOptions.options.filterKeys { key =>
          key.equalsIgnoreCase(DeltaOptions.MAX_RECORDS_PER_FILE) ||
            key.equalsIgnoreCase(DeltaOptions.COMPRESSION)
        }.toMap
    }) + (DeltaOptions.WRITE_PARTITION_COLUMNS -> writePartitionColumns.toString)

    try {
      DeltaFileFormatWriter.write(
        sparkSession = sparkSession,
        plan = physicalPlan,
        fileFormat = deltaLog.fileFormat(protocol, metadata), // TODO support changing formats.
        committer = committer,
        outputSpec = outputSpec,
        // scalastyle:off deltahadoopconfiguration
        hadoopConf = sparkSession.sessionState.newHadoopConfWithOptions(
          metadata.configuration ++ deltaLog.options),
        // scalastyle:on deltahadoopconfiguration
        partitionColumns = partitioningColumns,
        bucketSpec = None,
        statsTrackers = statsTrackers :+ basicWriteJobStatsTracker(deltaLog.newDeltaHadoopConf()),
        options = options)
    } catch {
      case InnerInvariantViolationException(violationException) =>
        // Pull an InvariantViolationException up to the top level if it was the root cause.
        throw violationException
    }

    Seq.empty
  }

  protected def withNewChildInternal(newChild: LogicalPlan): LogicalPlan = {
    copy(query = newChild)
  }

  /**
   * If there is any string partition column and there are constraints defined, add a projection to
   * convert empty string to null for that column. The empty strings will be converted to null
   * eventually even without this convert, but we want to do this earlier before check constraints
   * so that empty strings are correctly rejected. Note that this should not cause the downstream
   * logic in `FileFormatWriter` to add duplicate conversions because the logic there checks the
   * partition column using the original plan's output. When the plan is modified with additional
   * projections, the partition column check won't match and will not add more conversion.
   *
   * @param plan The original SparkPlan.
   * @param partCols The partition columns.
   * @param constraints The defined constraints.
   * @return A SparkPlan potentially modified with an additional projection on top of `plan`
   */
  protected def convertEmptyToNullIfNeeded(
      plan: SparkPlan,
      partCols: Seq[Attribute],
      constraints: Seq[Constraint]): SparkPlan = {
    if (!plan.session.conf.get(DeltaSQLConf.CONVERT_EMPTY_TO_NULL_FOR_STRING_PARTITION_COL)) {
      return plan
    }
    // No need to convert if there are no constraints. The empty strings will be converted later by
    // FileFormatWriter and FileFormatDataWriter. Note that we might still do unnecessary convert
    // here as the constraints might not be related to the string partition columns. A precise
    // check will need to walk the constraints to see if such columns are really involved. It
    // doesn't seem to worth the effort.
    if (constraints.isEmpty) return plan

    val partSet = AttributeSet(partCols)
    var needConvert = false
    val projectList: Seq[NamedExpression] = plan.output.map {
      case p if partSet.contains(p) && p.dataType == StringType =>
        needConvert = true
        Alias(org.apache.spark.sql.catalyst.expressions.Empty2Null(p), p.name)()
      case attr => attr
    }
    if (needConvert) ProjectExec(projectList, plan) else plan
  }

  /**
   * Optimized writes can be enabled/disabled through the following order:
   *  - Through DataFrameWriter options
   *  - Through SQL configuration
   *  - Through the table parameter
   */
  private def shouldOptimizeWrite(
      writeOptions: Option[DeltaOptions], sessionConf: SQLConf): Boolean = {
    writeOptions.flatMap(_.optimizeWrite)
      .getOrElse(WriteIntoDeltaCommand.shouldOptimizeWrite(metadata, sessionConf))
  }
}

object WriteIntoDeltaCommand {
  def shouldOptimizeWrite(metadata: Metadata, sessionConf: SQLConf): Boolean = {
    sessionConf.getConf(DeltaSQLConf.DELTA_OPTIMIZE_WRITE_ENABLED)
      .orElse(DeltaConfigs.OPTIMIZE_WRITE.fromMetaData(metadata))
      .getOrElse(false)
  }
}
