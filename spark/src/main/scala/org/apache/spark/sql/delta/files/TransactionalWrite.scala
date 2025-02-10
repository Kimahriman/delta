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

package org.apache.spark.sql.delta.files

import scala.collection.mutable.ListBuffer

import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.actions._
import org.apache.spark.sql.delta.commands.WriteIntoDeltaCommand
import org.apache.spark.sql.delta.commands.cdc.CDCReader
import org.apache.spark.sql.delta.constraints.{Constraint, Constraints}
import org.apache.spark.sql.delta.hooks.AutoCompact
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.schema._
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.sources.DeltaSQLConf.DELTA_COLLECT_STATS_USING_TABLE_SCHEMA
import org.apache.spark.sql.delta.stats.{
  DeltaJobStatisticsTracker,
  StatisticsCollection
}
import org.apache.spark.sql.util.ScalaExtensions._
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.catalyst.types.DataTypeUtils.toAttributes
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.datasources.{BasicWriteJobStatsTracker, FileFormatWriter, WriteJobStatsTracker}
import org.apache.spark.sql.execution.streaming.IncrementalExecution
import org.apache.spark.sql.functions.{col, to_json}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.util.SerializableConfiguration

/**
 * Adds the ability to write files out as part of a transaction. Checks
 * are performed to ensure that the data being written matches either the
 * current metadata or the new metadata being set by this transaction.
 */
trait TransactionalWrite extends DeltaLogging { self: OptimisticTransactionImpl =>

  protected var hasWritten = false

  private[delta] val deltaDataSubdir =
    if (spark.sessionState.conf.getConf(DeltaSQLConf.WRITE_DATA_FILES_TO_SUBDIR)) {
      Some("data")
    } else None

  protected def getCommitter(outputPath: Path): DelayedCommitProtocol =
    new DelayedCommitProtocol("delta", outputPath.toString, None, deltaDataSubdir)

  /** Makes the output attributes nullable, so that we don't write unreadable parquet files. */
  protected def makeOutputNullable(output: Seq[Attribute]): Seq[Attribute] = {
    output.map {
      case ref: AttributeReference =>
        val nullableDataType = SchemaUtils.typeAsNullable(ref.dataType)
        ref.copy(dataType = nullableDataType, nullable = true)(ref.exprId, ref.qualifier)
      case attr => attr.withNullability(true)
    }
  }

  /** Replace the output attributes with the physical mapping information. */
  protected def mapColumnAttributes(
      output: Seq[Attribute],
      mappingMode: DeltaColumnMappingMode): Seq[Attribute] = {
    DeltaColumnMapping.createPhysicalAttributes(output, metadata.schema, mappingMode)
  }

  /**
   * Used to perform all required normalizations before writing out the data.
   * Returns the QueryExecution to execute.
   */
  protected def normalizeData(
      deltaLog: DeltaLog,
      options: Option[DeltaOptions],
      data: DataFrame): (QueryExecution, Seq[Attribute], Seq[Constraint], Set[String]) = {
    val (normalizedSchema, output, constraints, trackHighWaterMarks) = normalizeSchema(
      deltaLog, options, data)

    (normalizedSchema.queryExecution, output, constraints, trackHighWaterMarks)
  }

  /**
   * Normalize the schema of the query, and returns the updated DataFrame. If the table has
   * generated columns and users provide these columns in the output, we will also return
   * constraints that should be respected. If any constraints are returned, the caller should apply
   * these constraints when writing data.
   *
   * Note: The schema of the DataFrame may not match the attributes we return as the
   * output schema. This is because streaming queries create `IncrementalExecution`, which cannot be
   * further modified. We can however have the Parquet writer use the physical plan from
   * `IncrementalExecution` and the output schema provided through the attributes.
   */
  protected def normalizeSchema(
      deltaLog: DeltaLog,
      options: Option[DeltaOptions],
      data: DataFrame): (DataFrame, Seq[Attribute], Seq[Constraint], Set[String]) = {
    val normalizedData = SchemaUtils.normalizeColumnNames(
      deltaLog, metadata.schema, data
    )

    // Validate that write columns for Row IDs have the correct name.
    RowId.throwIfMaterializedRowIdColumnNameIsInvalid(
      normalizedData, metadata, protocol, deltaLog.tableId)

    val nullAsDefault = options.isDefined &&
      options.get.options.contains(ColumnWithDefaultExprUtils.USE_NULL_AS_DEFAULT_DELTA_OPTION)
    val enforcesDefaultExprs = ColumnWithDefaultExprUtils.tableHasDefaultExpr(
      protocol, metadata, nullAsDefault)
    val (dataWithDefaultExprs, generatedColumnConstraints, trackHighWaterMarks) =
      if (enforcesDefaultExprs) {
        ColumnWithDefaultExprUtils.addDefaultExprsOrReturnConstraints(
          deltaLog,
          protocol,
          // We need the original query execution if this is a streaming query, because
          // `normalizedData` may add a new projection and change its type.
          data.queryExecution,
          metadata.schema,
          normalizedData,
          nullAsDefault)
      } else {
        (normalizedData, Nil, Set[String]())
      }
    val cleanedData = SchemaUtils.dropNullTypeColumns(dataWithDefaultExprs)
    val finalData = if (cleanedData.schema != dataWithDefaultExprs.schema) {
      // This must be batch execution as DeltaSink doesn't accept NullType in micro batch DataFrame.
      // For batch executions, we need to use the latest DataFrame query execution
      cleanedData
    } else if (enforcesDefaultExprs) {
      dataWithDefaultExprs
    } else {
      assert(
        normalizedData == dataWithDefaultExprs,
        "should not change data when there is no generate column")
      normalizedData
    }
    val nullableOutput = makeOutputNullable(cleanedData.queryExecution.analyzed.output)
    val columnMapping = metadata.columnMappingMode
    // Check partition column errors
    checkPartitionColumns(
      metadata.partitionSchema, nullableOutput, nullableOutput.length < data.schema.size
    )
    // Rewrite column physical names if using a mapping mode
    val mappedOutput = if (columnMapping == NoMapping) nullableOutput else {
      mapColumnAttributes(nullableOutput, columnMapping)
    }
    (finalData, mappedOutput, generatedColumnConstraints, trackHighWaterMarks)
  }

  protected def checkPartitionColumns(
      partitionSchema: StructType,
      output: Seq[Attribute],
      colsDropped: Boolean): Unit = {
    val partitionColumns: Seq[Attribute] = partitionSchema.map { col =>
      // schema is already normalized, therefore we can do an equality check
      output.find(f => f.name == col.name).getOrElse(
        throw DeltaErrors.partitionColumnNotFoundException(col.name, output)
      )
    }
    if (partitionColumns.nonEmpty && partitionColumns.length == output.length) {
      throw DeltaErrors.nonPartitionColumnAbsentException(colsDropped)
    }
  }

  protected def getPartitioningColumns(
      partitionSchema: StructType,
      output: Seq[Attribute]): Seq[Attribute] = {
    val partitionColumns: Seq[Attribute] = partitionSchema.map { col =>
      // schema is already normalized, therefore we can do an equality check
      // we have already checked for missing columns, so the fields must exist
      output.find(f => f.name == col.name).get
    }
    partitionColumns
  }

  def writeFiles(
      data: Dataset[_],
      additionalConstraints: Seq[Constraint]): Seq[FileAction] = {
    writeFiles(data, None, additionalConstraints)
  }

  def writeFiles(
      data: Dataset[_],
      writeOptions: Option[DeltaOptions]): Seq[FileAction] = {
    writeFiles(data, writeOptions, Nil)
  }

  def writeFiles(data: Dataset[_]): Seq[FileAction] = {
    writeFiles(data, Nil)
  }

  def writeFiles(
      data: Dataset[_],
      deltaOptions: Option[DeltaOptions],
      additionalConstraints: Seq[Constraint]): Seq[FileAction] = {
    writeFiles(data, deltaOptions, isOptimize = false, additionalConstraints)
  }

  /**
   * Returns a tuple of (data, partition schema). For CDC writes, a `__is_cdc` column is added to
   * the data and `__is_cdc=true/false` is added to the front of the partition schema.
   */
  protected def performCDCPartition(inputData: Dataset[_]): (DataFrame, StructType) = {
    // If this is a CDC write, we need to generate the CDC_PARTITION_COL in order to properly
    // dispatch rows between the main table and CDC event records. This is a virtual partition
    // and will be stripped out later in [[DelayedCommitProtocolEdge]].
    // Note that the ordering of the partition schema is relevant - CDC_PARTITION_COL must
    // come first in order to ensure CDC data lands in the right place.
    if (CDCReader.isCDCEnabledOnTable(metadata, spark) &&
      inputData.schema.fieldNames.contains(CDCReader.CDC_TYPE_COLUMN_NAME)) {
      val augmentedData = inputData.withColumn(
        CDCReader.CDC_PARTITION_COL, col(CDCReader.CDC_TYPE_COLUMN_NAME).isNotNull)
      val partitionSchema = StructType(
        StructField(CDCReader.CDC_PARTITION_COL, StringType) +: metadata.physicalPartitionSchema)
      (augmentedData, partitionSchema)
    } else {
      (inputData.toDF(), metadata.physicalPartitionSchema)
    }
  }

  /**
   * Return a tuple of (outputStatsCollectionSchema, statsCollectionSchema).
   * outputStatsCollectionSchema is the data source schema from DataFrame used for stats collection.
   * It contains the columns in the DataFrame output, excluding the partition columns.
   * tableStatsCollectionSchema is the schema to collect stats for. It contains the columns in the
   * table schema, excluding the partition columns.
   * Note: We only collect NULL_COUNT stats (as the number of rows) for the columns in
   * statsCollectionSchema but missing in outputStatsCollectionSchema
   */
  protected def getStatsSchema(
    dataFrameOutput: Seq[Attribute],
    partitionSchema: StructType): (Seq[Attribute], Seq[Attribute]) = {
    val partitionColNames = partitionSchema.map(_.name).toSet

    // The outputStatsCollectionSchema comes from DataFrame output
    // schema should be normalized, therefore we can do an equality check
    val outputStatsCollectionSchema = dataFrameOutput
      .filterNot(c => partitionColNames.contains(c.name))

    // The tableStatsCollectionSchema comes from table schema
    val statsTableSchema = toAttributes(metadata.schema)
    val mappedStatsTableSchema = if (metadata.columnMappingMode == NoMapping) {
      statsTableSchema
    } else {
      mapColumnAttributes(statsTableSchema, metadata.columnMappingMode)
    }

    // It's important to first do the column mapping and then drop the partition columns
    val tableStatsCollectionSchema = mappedStatsTableSchema
      .filterNot(c => partitionColNames.contains(c.name))

    (outputStatsCollectionSchema, tableStatsCollectionSchema)
  }

  /**
   * Returns a resolved `statsCollection.statsCollector` expression with `statsDataSchema`
   * attributes re-resolved to be used for writing Delta file stats.
   */
  protected def getStatsColExpr(
      statsDataSchema: Seq[Attribute],
      statsCollection: StatisticsCollection): (Expression, Seq[Attribute]) = {
    val resolvedPlan = Dataset.ofRows(spark, LocalRelation(statsDataSchema))
      .select(to_json(statsCollection.statsCollector))
      .queryExecution.analyzed

    // We have to use the new attributes with regenerated attribute IDs, because the Analyzer
    // doesn't guarantee that attributes IDs will stay the same
    val newStatsDataSchema = resolvedPlan.children.head.output

    resolvedPlan.expressions.head -> newStatsDataSchema
  }


  /** Return the pair of optional stats tracker and stats collection class */
  protected def getOptionalStatsTrackerAndStatsCollection(
      output: Seq[Attribute],
      outputPath: Path,
      partitionSchema: StructType, data: DataFrame): (
        Option[DeltaJobStatisticsTracker],
        Option[StatisticsCollection]) = {
    // check whether we should collect Delta stats
    val collectStats =
      (spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_COLLECT_STATS)
      )

    if (collectStats) {
      val (outputStatsCollectionSchema, tableStatsCollectionSchema) =
        getStatsSchema(output, partitionSchema)

      val statsCollection = new StatisticsCollection {
        override val columnMappingMode: DeltaColumnMappingMode = metadata.columnMappingMode
        override def tableSchema: StructType = metadata.schema
        override def outputTableStatsSchema: StructType = {
          // If collecting stats uses the table schema, then we pass in tableStatsCollectionSchema;
          // otherwise, pass in outputStatsCollectionSchema to collect stats using the DataFrame
          // schema.
          if (spark.sessionState.conf.getConf(DELTA_COLLECT_STATS_USING_TABLE_SCHEMA)) {
            tableStatsCollectionSchema.toStructType
          } else {
            outputStatsCollectionSchema.toStructType
          }
        }
        override def outputAttributeSchema: StructType = outputStatsCollectionSchema.toStructType
        override val spark: SparkSession = data.sparkSession
        override val statsColumnSpec = StatisticsCollection.configuredDeltaStatsColumnSpec(metadata)
        override val protocol: Protocol = newProtocol.getOrElse(snapshot.protocol)
      }
      val (statsColExpr, newOutputStatsCollectionSchema) =
        getStatsColExpr(outputStatsCollectionSchema, statsCollection)

      (Some(new DeltaJobStatisticsTracker(deltaLog.newDeltaHadoopConf(),
                                          outputPath,
                                          newOutputStatsCollectionSchema,
                                          statsColExpr
        )),
       Some(statsCollection))
    } else {
      (None, None)
    }
  }


  /**
   * Writes out the dataframe after performing schema validation. Returns a list of
   * actions to append these files to the reservoir.
   *
   * @param inputData Data to write out.
   * @param writeOptions Options to decide how to write out the data.
   * @param isOptimize Whether the operation writing this is Optimize or not.
   * @param additionalConstraints Additional constraints on the write.
   */
  def writeFiles(
      inputData: Dataset[_],
      writeOptions: Option[DeltaOptions],
      isOptimize: Boolean,
      additionalConstraints: Seq[Constraint]): Seq[FileAction] = {
    hasWritten = true

    val spark = inputData.sparkSession
    val (data, partitionSchema) = performCDCPartition(inputData)
    val outputPath = deltaLog.dataPath

    val (queryExecution, output, generatedColumnConstraints, trackFromData) =
      normalizeData(deltaLog, writeOptions, data)
    // Use the track set from the transaction if set,
    // otherwise use the track set from `normalizeData()`.
    val trackIdentityHighWaterMarks = trackHighWaterMarks.getOrElse(trackFromData)

    val partitioningColumns = getPartitioningColumns(partitionSchema, output)

    val committer = getCommitter(outputPath)

    val (statsDataSchema, _) = getStatsSchema(output, partitionSchema)

    // If Statistics Collection is enabled, then create a stats tracker that will be injected during
    // the FileFormatWriter.write call below and will collect per-file stats using
    // StatisticsCollection
    val (optionalStatsTracker, _) = getOptionalStatsTrackerAndStatsCollection(output, outputPath,
      partitionSchema, data)


    val constraints =
      Constraints.getAll(metadata, spark) ++ generatedColumnConstraints ++ additionalConstraints

    val identityTrackerOpt = IdentityColumn.createIdentityColumnStatsTracker(
      spark,
      deltaLog.newDeltaHadoopConf(),
      outputPath,
      metadata.schema,
      statsDataSchema,
      trackIdentityHighWaterMarks
    )

    val statsTrackers: ListBuffer[WriteJobStatsTracker] = ListBuffer()

    if (spark.conf.get(DeltaSQLConf.DELTA_HISTORY_METRICS_ENABLED)) {
      val basicWriteJobStatsTracker = new BasicWriteJobStatsTracker(
        new SerializableConfiguration(deltaLog.newDeltaHadoopConf()),
        BasicWriteJobStatsTracker.metrics)
      registerSQLMetrics(spark, basicWriteJobStatsTracker.driverSideMetrics)
      statsTrackers.append(basicWriteJobStatsTracker)
    }

    val outputSpec = FileFormatWriter.OutputSpec(
      deltaLog.dataPath.toString,
      Map.empty,
      output)

    val writeInto = WriteIntoDeltaCommand(
      deltaLog,
      queryExecution.analyzed,
      outputSpec,
      writeOptions,
      isOptimize,
      protocol,
      metadata,
      committer,
      partitioningColumns,
      constraints,
      statsTrackers = optionalStatsTracker.toSeq
        ++ statsTrackers
        ++ identityTrackerOpt.toSeq
    )
    val writeExecution = queryExecution match {
      case i: IncrementalExecution =>
        writeInto.run(spark, i.executedPlan)
      case q =>
        new QueryExecution(spark, writeInto).assertCommandExecuted()
    }

    statsTrackers.foreach {
      case tracker: BasicWriteJobStatsTracker =>
        val numOutputRowsOpt = tracker.driverSideMetrics.get("numOutputRows").map(_.value)
        IdentityColumn.logTableWrite(snapshot, trackIdentityHighWaterMarks, numOutputRowsOpt)
      case _ => ()
    }

    var resultFiles =
      (if (optionalStatsTracker.isDefined) {
        committer.addedStatuses.map { a =>
          a.copy(stats = optionalStatsTracker.map(
            _.recordedStats(a.toPath.getName)).getOrElse(a.stats))
        }
      }
      else {
        committer.addedStatuses
      })
      .filter {
      // In some cases, we can write out an empty `inputData`. Some examples of this (though, they
      // may be fixed in the future) are the MERGE command when you delete with empty source, or
      // empty target, or on disjoint tables. This is hard to catch before the write without
      // collecting the DF ahead of time. Instead, we can return only the AddFiles that
      // a) actually add rows, or
      // b) don't have any stats so we don't know the number of rows at all
      case a: AddFile => a.numLogicalRecords.forall(_ > 0)
      case _ => true
    }

    // add [[AddFile.Tags.ICEBERG_COMPAT_VERSION.name]] tags to addFiles
    if (IcebergCompatV2.isEnabled(metadata)) {
      resultFiles = resultFiles.map { addFile =>
        val tags = if (addFile.tags != null) addFile.tags else Map.empty[String, String]
        addFile.copy(tags = tags + (AddFile.Tags.ICEBERG_COMPAT_VERSION.name -> "2"))
      }
    }


    if (resultFiles.nonEmpty && !isOptimize) registerPostCommitHook(AutoCompact)
    // Record the updated high water marks to be used during transaction commit.
    identityTrackerOpt.ifDefined { tracker =>
      updatedIdentityHighWaterMarks.appendAll(tracker.highWaterMarks.toSeq)
    }

    resultFiles.toSeq ++ committer.changeFiles
  }
}
