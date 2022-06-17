#
# Copyright (2021) The Delta Lake Project Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from pathlib import Path
import unittest
import os
from typing import List, Set, Dict, Optional, Any, Callable, Union, Tuple

from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql.column import _to_seq  # type: ignore[attr-defined]
from pyspark.sql.functions import col, lit, expr
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DataType
from pyspark.sql.utils import AnalysisException, ParseException

from chispa import assert_df_equality  # type: ignore[import]
import pytest

from delta.tables import DeltaTable, DeltaTableBuilder, DeltaOptimizeBuilder
from delta.testing.utils import DeltaTestCase

@pytest.fixture(scope='function')
def database(spark_session: SparkSession, tmp_path: Path):
    path = tmp_path.as_posix()
    db_name = 'test_database'
    spark_session.sql(f"CREATE DATABASE {db_name} LOCATION '{path}'")
    spark_session.catalog.setCurrentDatabase(db_name)
    yield db_name
    spark_session.sql(f'DROP DATABASE {db_name} CASCADE')

def __writeDeltaTable(spark: SparkSession, path: Path, datalist: List[Tuple[Any, Any]]) -> DeltaTable:
    df = spark.createDataFrame(datalist, ["key", "value"])
    df.write.format("delta").save(path.as_uri())
    return DeltaTable.forPath(spark, path.as_uri())

def __overwriteDeltaTable(spark: SparkSession, path: Path, datalist: List[Tuple[Any, Any]],
                              schema: Union[StructType, List[str]] = ["key", "value"],
                              overwriteSchema: str = 'false') -> None:
    df = spark.createDataFrame(datalist, schema)
    df.write.format("delta") \
        .option('overwriteSchema', overwriteSchema) \
        .mode("overwrite") \
        .save(path.as_uri())

def __writeAsTable(spark: SparkSession, datalist: List[Tuple[Any, Any]], tblName: str) -> DeltaTable:
    df = spark.createDataFrame(datalist, ["key", "value"])
    df.write.format("delta").mode('overwrite').saveAsTable(tblName)
    return DeltaTable.forName(spark, tblName)

def __checkAnswer(spark: SparkSession,
                    df: DataFrame,
                    expectedAnswer: List[Any],
                    schema: Union[StructType, List[str]] = ["key", "value"],
                    ignoreOrder: bool = False) -> None:
    if not expectedAnswer:
        assert df.count() == 0
        return
    expectedDF = spark.createDataFrame(expectedAnswer, schema)
    # time.sleep(60)
    assert_df_equality(df.select('*'), expectedDF, ignore_row_order=ignoreOrder)

def __checkFileExists(path: str, fileName: str) -> bool:
    return os.path.exists(os.path.join(path, fileName))

def __createFile(path: str, fileName: str, content: Any) -> None:
        with open(os.path.join(path, fileName), 'w') as f:
            f.write(content)

def test_forPath(spark_session: SparkSession, tmp_path: Path) -> None:
    __writeDeltaTable(spark_session, tmp_path, [('a', 1), ('b', 2), ('c', 3)])
    dt = DeltaTable.forPath(spark_session, tmp_path.as_uri()).toDF()
    __checkAnswer(spark_session, dt, [('a', 1), ('b', 2), ('c', 3)])

def test_forName(spark_session: SparkSession) -> None:
    __writeAsTable(spark_session, [('a', 1), ('b', 2), ('c', 3)], "test")
    df = DeltaTable.forName(spark_session, "test").toDF()
    __checkAnswer(spark_session, df, [('a', 1), ('b', 2), ('c', 3)])

def test_delete(spark_session: SparkSession, tmp_path: Path) -> None:
    dt = __writeDeltaTable(spark_session, tmp_path, [('a', 1), ('b', 2), ('c', 3), ('d', 4)])

    # delete with condition as str
    dt.delete("key = 'a'")
    __checkAnswer(spark_session, dt.toDF(), [('b', 2), ('c', 3), ('d', 4)])

    # dt = DeltaTable.forPath(spark_session, tmp_path.as_uri())
    # delete with condition as Column
    dt.delete(col("key") == lit("b"))
    __checkAnswer(spark_session, dt.toDF(), [('c', 3), ('d', 4)])

    # delete without condition
    dt.delete()
    assert dt.toDF().count() == 0

    # bad args
    with pytest.raises(TypeError):
        dt.delete(condition=1)  # type: ignore[arg-type]

def test_generate(spark_session: SparkSession, tmp_path: Path) -> None:
    # create a delta table
    numFiles = 10
    path = tmp_path.as_uri()
    spark_session.range(100).repartition(numFiles).write.format("delta").save(path)
    dt = DeltaTable.forPath(spark_session, path)

    # Generate the symlink format manifest
    dt.generate("symlink_format_manifest")

    # check the contents of the manifest
    # NOTE: this is not a correctness test, we are testing correctness in the scala suite
    manifestPath = os.path.join(tmp_path.as_posix(), "_symlink_format_manifest", "manifest")
    files = []
    with open(manifestPath) as f:
        files = f.readlines()

    # the number of files we write should equal the number of lines in the manifest
    assert len(files) == numFiles

def test_update(spark_session: SparkSession, tmp_path: Path) -> None:
    dt = __writeDeltaTable(spark_session, tmp_path, [('a', 1), ('b', 2), ('c', 3), ('d', 4)])

    # update with condition as str and with set exprs as str
    dt.update("key = 'a' or key = 'b'", {"value": "1"})
    __checkAnswer(spark_session, dt.toDF(), [('a', 1), ('b', 1), ('c', 3), ('d', 4)])

    # update with condition as Column and with set exprs as Columns
    dt.update(expr("key = 'a' or key = 'b'"), {"value": expr("0")})
    __checkAnswer(spark_session, dt.toDF(), [('a', 0), ('b', 0), ('c', 3), ('d', 4)])

    # update without condition
    dt.update(set={"value": "200"})
    __checkAnswer(spark_session, dt.toDF(), [('a', 200), ('b', 200), ('c', 200), ('d', 200)])

    # bad args
    with pytest.raises(ValueError, match = "cannot be None"):
        dt.update({"value": "200"})  # type: ignore[call-overload]

    with pytest.raises(ValueError, match = "cannot be None"):
        dt.update(condition='a')  # type: ignore[call-overload]

    with pytest.raises(TypeError, match = "must be a dict"):
        dt.update(set=1)  # type: ignore[call-overload]

    with pytest.raises(TypeError, match = "must be a Spark SQL Column or a string"):
        dt.update(1, {})  # type: ignore[call-overload]

    with pytest.raises(TypeError, match = "Values of dict in .* must contain only"):
        dt.update(set={"value": 1})  # type: ignore[dict-item]

    with pytest.raises(TypeError, match = "Keys of dict in .* must contain only"):
        dt.update(set={1: ""})  # type: ignore[dict-item]

    with pytest.raises(TypeError):
        dt.update(set=1)  # type: ignore[call-overload]

def test_merge(spark_session: SparkSession, tmp_path: Path) -> None:
    dt = __writeDeltaTable(spark_session, tmp_path, [('a', 1), ('b', 2), ('c', 3), ('d', 4)])
    source = spark_session.createDataFrame([('a', -1), ('b', 0), ('e', -5), ('f', -6)], ["k", "v"])

    def reset_table() -> None:
        __overwriteDeltaTable(spark_session, tmp_path, [('a', 1), ('b', 2), ('c', 3), ('d', 4)])

    # ============== Test basic syntax ==============

    # String expressions in merge condition and dicts
    reset_table()
    dt.merge(source, "key = k") \
        .whenMatchedUpdate(set={"value": "v + 0"}) \
        .whenNotMatchedInsert(values={"key": "k", "value": "v + 0"}) \
        .execute()
    __checkAnswer(spark_session, dt.toDF(),
                        ([('a', -1), ('b', 0), ('c', 3), ('d', 4), ('e', -5), ('f', -6)]))

    # Column expressions in merge condition and dicts
    reset_table()
    dt.merge(source, expr("key = k")) \
        .whenMatchedUpdate(set={"value": col("v") + 0}) \
        .whenNotMatchedInsert(values={"key": "k", "value": col("v") + 0}) \
        .execute()
    __checkAnswer(spark_session, dt.toDF(),
                        ([('a', -1), ('b', 0), ('c', 3), ('d', 4), ('e', -5), ('f', -6)]))

    # ============== Test clause conditions ==============

    # String expressions in all conditions and dicts
    reset_table()
    dt.merge(source, "key = k") \
        .whenMatchedUpdate(condition="k = 'a'", set={"value": "v + 0"}) \
        .whenMatchedDelete(condition="k = 'b'") \
        .whenNotMatchedInsert(condition="k = 'e'", values={"key": "k", "value": "v + 0"}) \
        .execute()
    __checkAnswer(spark_session, dt.toDF(), ([('a', -1), ('c', 3), ('d', 4), ('e', -5)]))

    # Column expressions in all conditions and dicts
    reset_table()
    dt.merge(source, expr("key = k")) \
        .whenMatchedUpdate(
            condition=expr("k = 'a'"),
            set={"value": col("v") + 0}) \
        .whenMatchedDelete(condition=expr("k = 'b'")) \
        .whenNotMatchedInsert(
            condition=expr("k = 'e'"),
            values={"key": "k", "value": col("v") + 0}) \
        .execute()
    __checkAnswer(spark_session, dt.toDF(), ([('a', -1), ('c', 3), ('d', 4), ('e', -5)]))

    # Positional arguments
    reset_table()
    dt.merge(source, "key = k") \
        .whenMatchedUpdate("k = 'a'", {"value": "v + 0"}) \
        .whenMatchedDelete("k = 'b'") \
        .whenNotMatchedInsert("k = 'e'", {"key": "k", "value": "v + 0"}) \
        .execute()
    __checkAnswer(spark_session, dt.toDF(), ([('a', -1), ('c', 3), ('d', 4), ('e', -5)]))

    # ============== Test updateAll/insertAll ==============

    # No clause conditions and insertAll/updateAll + aliases
    reset_table()
    dt.alias("t") \
        .merge(source.toDF("key", "value").alias("s"), expr("t.key = s.key")) \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()
    __checkAnswer(spark_session, dt.toDF(),
                        ([('a', -1), ('b', 0), ('c', 3), ('d', 4), ('e', -5), ('f', -6)]))

    # String expressions in all clause conditions and insertAll/updateAll + aliases
    reset_table()
    dt.alias("t") \
        .merge(source.toDF("key", "value").alias("s"), "s.key = t.key") \
        .whenMatchedUpdateAll("s.key = 'a'") \
        .whenNotMatchedInsertAll("s.key = 'e'") \
        .execute()
    __checkAnswer(spark_session, dt.toDF(), ([('a', -1), ('b', 2), ('c', 3), ('d', 4), ('e', -5)]))

    # Column expressions in all clause conditions and insertAll/updateAll + aliases
    reset_table()
    dt.alias("t") \
        .merge(source.toDF("key", "value").alias("s"), expr("t.key = s.key")) \
        .whenMatchedUpdateAll(expr("s.key = 'a'")) \
        .whenNotMatchedInsertAll(expr("s.key = 'e'")) \
        .execute()
    __checkAnswer(spark_session, dt.toDF(), ([('a', -1), ('b', 2), ('c', 3), ('d', 4), ('e', -5)]))

    # ============== Test bad args ==============
    # ---- bad args in merge()
    with pytest.raises(TypeError, match = "must be DataFrame"):
        dt.merge(1, "key = k")  # type: ignore[arg-type]

    with pytest.raises(TypeError, match = "must be a Spark SQL Column or a string"):
        dt.merge(source, 1)  # type: ignore[arg-type]

    # ---- bad args in whenMatchedUpdate()
    with pytest.raises(ValueError, match = "cannot be None"):
        (dt  # type: ignore[call-overload]
            .merge(source, "key = k")
            .whenMatchedUpdate({"value": "v"}))

    with pytest.raises(ValueError, match = "cannot be None"):
        (dt  # type: ignore[call-overload]
            .merge(source, "key = k")
            .whenMatchedUpdate(1))

    with pytest.raises(ValueError, match = "cannot be None"):
        (dt  # type: ignore[call-overload]
            .merge(source, "key = k")
            .whenMatchedUpdate(condition="key = 'a'"))

    with pytest.raises(TypeError, match = "must be a Spark SQL Column or a string"):
        (dt  # type: ignore[call-overload]
            .merge(source, "key = k")
            .whenMatchedUpdate(1, {"value": "v"}))

    with pytest.raises(TypeError, match = "must be a dict"):
        (dt  # type: ignore[call-overload]
            .merge(source, "key = k")
            .whenMatchedUpdate("k = 'a'", 1))

    with pytest.raises(TypeError, match = "Values of dict in .* must contain only"):
        (dt
            .merge(source, "key = k")
            .whenMatchedUpdate(set={"value": 1}))  # type: ignore[dict-item]

    with pytest.raises(TypeError, match = "Keys of dict in .* must contain only"):
        (dt
            .merge(source, "key = k")
            .whenMatchedUpdate(set={1: ""}))  # type: ignore[dict-item]

    with pytest.raises(TypeError):
        (dt  # type: ignore[call-overload]
            .merge(source, "key = k")
            .whenMatchedUpdate(set="k = 'a'", condition={"value": 1}))

    # bad args in whenMatchedDelete()
    with pytest.raises(TypeError, match = "must be a Spark SQL Column or a string"):
        dt.merge(source, "key = k").whenMatchedDelete(1)  # type: ignore[arg-type]

    # ---- bad args in whenNotMatchedInsert()
    with pytest.raises(ValueError, match = "cannot be None"):
        (dt  # type: ignore[call-overload]
            .merge(source, "key = k")
            .whenNotMatchedInsert({"value": "v"}))

    with pytest.raises(ValueError, match = "cannot be None"):
        dt.merge(source, "key = k").whenNotMatchedInsert(1)  # type: ignore[call-overload]

    with pytest.raises(ValueError, match = "cannot be None"):
        (dt  # type: ignore[call-overload]
            .merge(source, "key = k")
            .whenNotMatchedInsert(condition="key = 'a'"))

    with pytest.raises(TypeError, match = "must be a Spark SQL Column or a string"):
        (dt  # type: ignore[call-overload]
            .merge(source, "key = k")
            .whenNotMatchedInsert(1, {"value": "v"}))

    with pytest.raises(TypeError, match = "must be a dict"):
        (dt  # type: ignore[call-overload]
            .merge(source, "key = k")
            .whenNotMatchedInsert("k = 'a'", 1))

    with pytest.raises(TypeError, match = "Values of dict in .* must contain only"):
        (dt
            .merge(source, "key = k")
            .whenNotMatchedInsert(values={"value": 1}))  # type: ignore[dict-item]

    with pytest.raises(TypeError, match = "Keys of dict in .* must contain only"):
        (dt
            .merge(source, "key = k")
            .whenNotMatchedInsert(values={1: "value"}))  # type: ignore[dict-item]

    with pytest.raises(TypeError):
        (dt  # type: ignore[call-overload]
            .merge(source, "key = k")
            .whenNotMatchedInsert(values="k = 'a'", condition={"value": 1}))

def test_history(spark_session: SparkSession, tmp_path: Path) -> None:
    dt = __writeDeltaTable(spark_session, tmp_path, [('a', 1), ('b', 2), ('c', 3)])
    __overwriteDeltaTable(spark_session, tmp_path, [('a', 3), ('b', 2), ('c', 1)])
    operations = dt.history().select('operation')
    __checkAnswer(spark_session, operations,
                        [Row("WRITE"), Row("WRITE")],
                        StructType([StructField(
                            "operation", StringType(), True)]))

    lastMode = dt.history(1).select('operationParameters.mode')
    __checkAnswer(
        spark_session,
        lastMode,
        [Row("Overwrite")],
        StructType([StructField("mode", StringType(), True)]))

def test_vacuum(spark_session: SparkSession, tmp_path: Path) -> None:
    dt = __writeDeltaTable(spark_session, tmp_path, [('a', 1), ('b', 2), ('c', 3)])
    path = tmp_path.as_posix()
    __createFile(path, 'abc.txt', 'abcde')
    __createFile(path, 'bac.txt', 'abcdf')
    assert __checkFileExists(path, 'abc.txt')
    dt.vacuum()  # will not delete files as default retention is used.
    dt.vacuum(1000)  # test whether integers work

    assert __checkFileExists(path, 'bac.txt')
    retentionConf = "spark.databricks.delta.retentionDurationCheck.enabled"
    spark_session.conf.set(retentionConf, "false")
    dt.vacuum(0.0)
    spark_session.conf.set(retentionConf, "true")
    assert not __checkFileExists(path, 'bac.txt')
    assert not __checkFileExists(path, 'abc.txt')

def test_convertToDelta(spark_session: SparkSession, tmp_path: Path) -> None:
    df = spark_session.createDataFrame([('a', 1), ('b', 2), ('c', 3)], ["key", "value"])
    path = tmp_path.as_uri()
    path1 = os.path.join(path, 'test1')
    df.write.format("parquet").save(path1)
    dt = DeltaTable.convertToDelta(spark_session, f"parquet.`{path1}`")
    __checkAnswer(
        spark_session,
        spark_session.read.format("delta").load(path1),
        [('a', 1), ('b', 2), ('c', 3)])

    # test if convert to delta with partition columns work
    path2 = os.path.join(path, 'test2')
    df.write.partitionBy("value").format("parquet").save(path2)
    schema = StructType()
    schema.add("value", LongType(), True)
    dt = DeltaTable.convertToDelta(
        spark_session,
        f"parquet.`{path2}`",
        schema)
    __checkAnswer(
        spark_session,
        spark_session.read.format("delta").load(path2),
        [('a', 1), ('b', 2), ('c', 3)],
        ignoreOrder = True)
    assert isinstance(dt, DeltaTable)

    # convert to delta with partition column provided as a string
    path3 = os.path.join(path, 'test3')
    df.write.partitionBy("value").format("parquet").save(path3)
    dt = DeltaTable.convertToDelta(
        spark_session,
        f"parquet.`{path3}`",
        "value long")
    __checkAnswer(
        spark_session,
        spark_session.read.format("delta").load(path3),
        [('a', 1), ('b', 2), ('c', 3)],
        ignoreOrder = True)
    assert isinstance(dt, DeltaTable)

def test_isDeltaTable(spark_session: SparkSession, tmp_path: Path) -> None:
    path = tmp_path.as_uri()
    path1 = os.path.join(path, 'test1')
    df = spark_session.createDataFrame([('a', 1), ('b', 2), ('c', 3)], ["key", "value"])
    df.write.format("parquet").save(path1)
    path2 = os.path.join(path, 'test2')
    df.write.format("delta").save(path2)
    assert not DeltaTable.isDeltaTable(spark_session, path1)
    assert DeltaTable.isDeltaTable(spark_session, path2)

def __verify_table_schema(spark: SparkSession, tableName: str, schema: StructType, cols: List[str],
                            types: List[DataType], nullables: Set[str] = set(),
                            comments: Dict[str, str] = {},
                            properties: Dict[str, str] = {},
                            partitioningColumns: List[str] = [],
                            tblComment: Optional[str] = None) -> None:
    fields = []
    for i in range(len(cols)):
        col = cols[i]
        dataType = types[i]
        metadata = {}
        if col in comments:
            metadata["comment"] = comments[col]
        fields.append(StructField(col, dataType, col in nullables, metadata))
    assert (StructType(fields) == schema)
    if len(properties) > 0:
        tablePropertyMap: Dict[str, str] = (
            spark.sql(  # type: ignore[assignment, misc]
                "SHOW TBLPROPERTIES {}".format(tableName)
            )
            .rdd.collectAsMap())
        for key in properties:
            assert (key in tablePropertyMap)
            assert (tablePropertyMap[key] == properties[key])
    tableDetails = spark.sql("DESCRIBE DETAIL {}".format(tableName))\
        .collect()[0]
    assert(tableDetails.format == "delta")
    actualComment = tableDetails.description
    assert(actualComment == tblComment)
    partitionCols = tableDetails.partitionColumns
    assert(sorted(partitionCols) == sorted((partitioningColumns)))

def __verify_generated_column(spark: SparkSession, tableName: str, deltaTable: DeltaTable) -> None:
    cmd = "INSERT INTO {table} (col1, col2) VALUES (1, 11)".format(table=tableName)
    spark.sql(cmd)
    deltaTable.update(expr("col2 = 11"), {"col1": expr("2")})
    __checkAnswer(spark, deltaTable.toDF(), [(2, 12)], schema=["col1", "col2"])

def __build_delta_table(builder: DeltaTableBuilder) -> DeltaTable:
    return builder.addColumn("col1", "int", comment="foo", nullable=False) \
        .addColumn("col2", IntegerType(), generatedAlwaysAs="col1 + 10") \
        .property("foo", "bar") \
        .comment("comment") \
        .partitionedBy("col1").execute()

def __create_table(spark: SparkSession, ifNotExists: bool,
                    tableName: Optional[str] = None,
                    location: Optional[str] = None) -> DeltaTable:
    builder = DeltaTable.createIfNotExists(spark) if ifNotExists \
        else DeltaTable.create(spark)
    if tableName:
        builder = builder.tableName(tableName)
    if location:
        builder = builder.location(location)
    return __build_delta_table(builder)

def __replace_table(spark: SparkSession,
                    orCreate: bool,
                    tableName: Optional[str] = None,
                    location: Optional[str] = None) -> DeltaTable:
    builder = DeltaTable.createOrReplace(spark) if orCreate \
        else DeltaTable.replace(spark)
    if tableName:
        builder = builder.tableName(tableName)
    if location:
        builder = builder.location(location)
    return __build_delta_table(builder)

def test_create_table_with_existing_schema(spark_session: SparkSession, database: str) -> None:
    df = spark_session.createDataFrame([('a', 1), ('b', 2), ('c', 3)], ["key", "value"])
    deltaTable = DeltaTable.create(spark_session).tableName("test") \
        .addColumns(df.schema) \
        .addColumn("value2", dataType="int")\
        .partitionedBy(["value2", "value"])\
        .execute()
    __verify_table_schema(spark_session, "test",
                                deltaTable.toDF().schema,
                                ["key", "value", "value2"],
                                [StringType(), LongType(), IntegerType()],
                                nullables={"key", "value", "value2"},
                                partitioningColumns=["value", "value2"])

    # verify creating table with list of structFields
    deltaTable2 = DeltaTable.create(spark_session).tableName("test2").addColumns(
        df.schema.fields) \
        .addColumn("value2", dataType="int") \
        .partitionedBy("value2", "value")\
        .execute()
    __verify_table_schema(spark_session, "test2",
                                deltaTable2.toDF().schema,
                                ["key", "value", "value2"],
                                [StringType(), LongType(), IntegerType()],
                                nullables={"key", "value", "value2"},
                                partitioningColumns=["value", "value2"])

def test_create_replace_table_with_no_spark_session_passed(spark_session: SparkSession, database: str) -> None:
    # create table.
    deltaTable = DeltaTable.create().tableName("test")\
        .addColumn("value", dataType="int").execute()
    __verify_table_schema(spark_session, "test",
                            deltaTable.toDF().schema,
                            ["value"],
                            [IntegerType()],
                            nullables={"value"})

    # ignore existence with createIfNotExists
    deltaTable = DeltaTable.createIfNotExists().tableName("test") \
        .addColumn("value2", dataType="int").execute()
    __verify_table_schema(spark_session, "test",
                                deltaTable.toDF().schema,
                                ["value"],
                                [IntegerType()],
                                nullables={"value"})

    # replace table with replace
    deltaTable = DeltaTable.replace().tableName("test") \
        .addColumn("key", dataType="int").execute()
    __verify_table_schema(spark_session, "test",
                                deltaTable.toDF().schema,
                                ["key"],
                                [IntegerType()],
                                nullables={"key"})

    # replace with a new column again
    deltaTable = DeltaTable.createOrReplace().tableName("test") \
        .addColumn("col1", dataType="int").execute()

    __verify_table_schema(spark_session, "test",
                                deltaTable.toDF().schema,
                                ["col1"],
                                [IntegerType()],
                                nullables={"col1"})

def test_create_table_with_name_only(spark_session: SparkSession, database: str) -> None:
    for ifNotExists in (False, True):
        tableName = "testTable{}".format(ifNotExists)
        deltaTable = __create_table(spark_session, ifNotExists, tableName=tableName)

        __verify_table_schema(spark_session, tableName,
                                    deltaTable.toDF().schema,
                                    ["col1", "col2"],
                                    [LongType(), LongType()],
                                    nullables={"col2"},
                                    comments={"col1": "foo"},
                                    properties={"foo": "bar"},
                                    partitioningColumns=["col1"],
                                    tblComment="comment")
        # verify generated columns.
        __verify_generated_column(spark_session, tableName, deltaTable)

def test_create_table_with_location_only(spark_session: SparkSession, tmp_path: Path) -> None:
    for ifNotExists in (False, True):
        path = os.path.join(tmp_path.as_posix(), (ifNotExists))
        deltaTable = __create_table(spark_session, ifNotExists, location=path)

        __verify_table_schema(spark_session, "delta.`{}`".format(path),
                                    deltaTable.toDF().schema,
                                    ["col1", "col2"],
                                    [LongType(), LongType()],
                                    nullables={"col2"},
                                    comments={"col1": "foo"},
                                    partitioningColumns=["col1"],
                                    tblComment="comment")
        # verify generated columns.
        __verify_generated_column(spark_session, "delta.`{}`".format(path), deltaTable)

def test_create_table_with_name_and_location(spark_session: SparkSession, tmp_path: Path, database: str) -> None:
    for ifNotExists in (False, True):
        path = os.path.join(tmp_path.as_posix(), (ifNotExists))
        tableName = "testTable{}".format(ifNotExists)
        deltaTable = __create_table(spark_session,
            ifNotExists, tableName=tableName, location=path)

        __verify_table_schema(spark_session, tableName,
                                    deltaTable.toDF().schema,
                                    ["col1", "col2"],
                                    [LongType(), LongType()],
                                    nullables={"col2"},
                                    comments={"col1": "foo"},
                                    properties={"foo": "bar"},
                                    partitioningColumns=["col1"],
                                    tblComment="comment")
        # verify generated columns.
        __verify_generated_column(spark_session, tableName, deltaTable)

def test_create_table_behavior(spark_session: SparkSession, database: str) -> None:
    spark_session.sql("CREATE TABLE testTable (c1 int) USING DELTA")

    # Errors out if doesn't ignore.
    try:
        __create_table(spark_session, False, tableName="testTable")
    except AnalysisException as e:
        msg = e.desc
    assert msg == f"Table {database}.testTable already exists"

    # ignore table creation.
    __create_table(spark_session, True, tableName="testTable")
    schema = spark_session.read.format("delta").table("testTable").schema
    __verify_table_schema(spark_session, "testTable",
                                schema,
                                ["c1"],
                                [IntegerType()],
                                nullables={"c1"})

def test_replace_table_with_name_only(spark_session: SparkSession, database: str) -> None:
    for orCreate in (False, True):
        tableName = "testTable{}".format(orCreate)
        spark_session.sql(f"CREATE TABLE {tableName} (c1 int) USING DELTA")
        deltaTable = __replace_table(spark_session, orCreate, tableName=tableName)

        __verify_table_schema(spark_session, tableName,
                                    deltaTable.toDF().schema,
                                    ["col1", "col2"],
                                    [LongType(), LongType()],
                                    nullables={"col2"},
                                    comments={"col1": "foo"},
                                    properties={"foo": "bar"},
                                    partitioningColumns=["col1"],
                                    tblComment="comment")
        # verify generated columns.
        __verify_generated_column(spark_session, tableName, deltaTable)

def test_replace_table_with_location_only(spark_session: SparkSession, tmp_path: Path) -> None:
    for orCreate in (False, True):
        path = os.path.join(tmp_path.as_posix(), str(orCreate))
        __create_table(spark_session, False, location=path)
        deltaTable = __replace_table(spark_session, orCreate, location=path)

        __verify_table_schema(spark_session, f"delta.`{path}`",
                                    deltaTable.toDF().schema,
                                    ["col1", "col2"],
                                    [LongType(), LongType()],
                                    nullables={"col2"},
                                    comments={"col1": "foo"},
                                    properties={"foo": "bar"},
                                    partitioningColumns=["col1"],
                                    tblComment="comment")
        # verify generated columns.
        __verify_generated_column(spark_session, f"delta.`{path}`", deltaTable)

class DeltaTableTests(DeltaTestCase):


    

    

    

    

    def test_replace_table_with_name_and_location(self) -> None:
        for orCreate in (False, True):
            path = self.tempFile + str(orCreate)
            tableName = "testTable{}".format(orCreate)
            self.spark.sql("CREATE TABLE {} (col int) USING DELTA LOCATION '{}'"
                           .format(tableName, path))
            deltaTable = self.__replace_table(
                orCreate, tableName=tableName, location=path)

            self.__verify_table_schema(tableName,
                                       deltaTable.toDF().schema,
                                       ["col1", "col2"],
                                       [IntegerType(), IntegerType()],
                                       nullables={"col2"},
                                       comments={"col1": "foo"},
                                       properties={"foo": "bar"},
                                       partitioningColumns=["col1"],
                                       tblComment="comment")
            # verify generated columns.
            self.__verify_generated_column(tableName, deltaTable)
            self.spark.sql("DROP TABLE IF EXISTS {}".format(tableName))

    def test_replace_table_behavior(self) -> None:
        msg = None
        try:
            self.__replace_table(False, tableName="testTable")
        except AnalysisException as e:
            msg = e.desc
        assert msg is not None
        assert (msg.startswith(
            "Table default.testTable cannot be replaced as it did not exist."))
        deltaTable = self.__replace_table(True, tableName="testTable")
        self.__verify_table_schema("testTable",
                                   deltaTable.toDF().schema,
                                   ["col1", "col2"],
                                   [IntegerType(), IntegerType()],
                                   nullables={"col2"},
                                   comments={"col1": "foo"},
                                   properties={"foo": "bar"},
                                   partitioningColumns=["col1"],
                                   tblComment="comment")

    def test_verify_paritionedBy_compatibility(self) -> None:
        tableBuilder = DeltaTable.create(self.spark).tableName("testTable") \
            .addColumn("col1", "int", comment="foo", nullable=False) \
            .addColumn("col2", IntegerType(), generatedAlwaysAs="col1 + 10") \
            .property("foo", "bar") \
            .comment("comment")
        tableBuilder._jbuilder = tableBuilder._jbuilder.partitionedBy(
            _to_seq(self.spark._sc, ["col1"])  # type: ignore[attr-defined]
        )
        deltaTable = tableBuilder.execute()
        self.__verify_table_schema("testTable",
                                   deltaTable.toDF().schema,
                                   ["col1", "col2"],
                                   [IntegerType(), IntegerType()],
                                   nullables={"col2"},
                                   comments={"col1": "foo"},
                                   properties={"foo": "bar"},
                                   partitioningColumns=["col1"],
                                   tblComment="comment")

    def test_delta_table_builder_with_bad_args(self) -> None:
        builder = DeltaTable.create(self.spark)

        # bad table name
        with self.assertRaises(TypeError):
            builder.tableName(1)  # type: ignore[arg-type]

        # bad location
        with self.assertRaises(TypeError):
            builder.location(1)  # type: ignore[arg-type]

        # bad comment
        with self.assertRaises(TypeError):
            builder.comment(1)  # type: ignore[arg-type]

        # bad column name
        with self.assertRaises(TypeError):
            builder.addColumn(1, "int")  # type: ignore[arg-type]

        # bad datatype.
        with self.assertRaises(TypeError):
            builder.addColumn("a", 1)  # type: ignore[arg-type]

        # bad column datatype - can't be pared
        with self.assertRaises(ParseException):
            builder.addColumn("a", "1")

        # bad comment
        with self.assertRaises(TypeError):
            builder.addColumn("a", "int", comment=1)  # type: ignore[arg-type]

        # bad generatedAlwaysAs
        with self.assertRaises(TypeError):
            builder.addColumn("a", "int", generatedAlwaysAs=1)  # type: ignore[arg-type]

        # bad nullable
        with self.assertRaises(TypeError):
            builder.addColumn("a", "int", nullable=1)  # type: ignore[arg-type]

        # bad existing schema
        with self.assertRaises(TypeError):
            builder.addColumns(1)  # type: ignore[arg-type]

        # bad existing schema.
        with self.assertRaises(TypeError):
            builder.addColumns([StructField("1", IntegerType()), 1])  # type: ignore[list-item]

        # bad partitionedBy col name
        with self.assertRaises(TypeError):
            builder.partitionedBy(1)  # type: ignore[call-overload]

        with self.assertRaises(TypeError):
            builder.partitionedBy(1, "1")   # type: ignore[call-overload]

        with self.assertRaises(TypeError):
            builder.partitionedBy([1])  # type: ignore[list-item]

        # bad property key
        with self.assertRaises(TypeError):
            builder.property(1, "1")  # type: ignore[arg-type]

        # bad property value
        with self.assertRaises(TypeError):
            builder.property("1", 1)  # type: ignore[arg-type]

    def test_protocolUpgrade(self) -> None:
        try:
            self.spark.conf.set('spark.databricks.delta.minWriterVersion', '2')
            self.spark.conf.set('spark.databricks.delta.minReaderVersion', '1')
            self.__writeDeltaTable([('a', 1), ('b', 2), ('c', 3), ('d', 4)])
            dt = DeltaTable.forPath(self.spark, self.tempFile)
            dt.upgradeTableProtocol(1, 3)
        finally:
            self.spark.conf.unset('spark.databricks.delta.minWriterVersion')
            self.spark.conf.unset('spark.databricks.delta.minReaderVersion')

        # cannot downgrade once upgraded
        failed = False
        try:
            dt.upgradeTableProtocol(1, 2)
        except BaseException:
            failed = True
        self.assertTrue(failed, "The upgrade should have failed, because downgrades aren't allowed")

        # bad args
        with self.assertRaisesRegex(ValueError, "readerVersion"):
            dt.upgradeTableProtocol("abc", 3)  # type: ignore[arg-type]
        with self.assertRaisesRegex(ValueError, "readerVersion"):
            dt.upgradeTableProtocol([1], 3)  # type: ignore[arg-type]
        with self.assertRaisesRegex(ValueError, "readerVersion"):
            dt.upgradeTableProtocol([], 3)  # type: ignore[arg-type]
        with self.assertRaisesRegex(ValueError, "readerVersion"):
            dt.upgradeTableProtocol({}, 3)  # type: ignore[arg-type]
        with self.assertRaisesRegex(ValueError, "writerVersion"):
            dt.upgradeTableProtocol(1, "abc")  # type: ignore[arg-type]
        with self.assertRaisesRegex(ValueError, "writerVersion"):
            dt.upgradeTableProtocol(1, [3])  # type: ignore[arg-type]
        with self.assertRaisesRegex(ValueError, "writerVersion"):
            dt.upgradeTableProtocol(1, [])  # type: ignore[arg-type]
        with self.assertRaisesRegex(ValueError, "writerVersion"):
            dt.upgradeTableProtocol(1, {})  # type: ignore[arg-type]

    def test_restore_to_version(self) -> None:
        self.__writeDeltaTable([('a', 1), ('b', 2)])
        self.__overwriteDeltaTable([('a', 3), ('b', 2)],
                                   schema=["key_new", "value_new"],
                                   overwriteSchema='true')

        overwritten = DeltaTable.forPath(self.spark, self.tempFile).toDF()
        self.__checkAnswer(overwritten,
                           [Row(key_new='a', value_new=3), Row(key_new='b', value_new=2)])

        DeltaTable.forPath(self.spark, self.tempFile).restoreToVersion(0)
        restored = DeltaTable.forPath(self.spark, self.tempFile).toDF()

        self.__checkAnswer(restored, [Row(key='a', value=1), Row(key='b', value=2)])

    def test_restore_to_timestamp(self) -> None:
        self.__writeDeltaTable([('a', 1), ('b', 2)])
        timestampToRestore = DeltaTable.forPath(self.spark, self.tempFile) \
            .history() \
            .head() \
            .timestamp \
            .strftime('%Y-%m-%d %H:%M:%S.%f')

        self.__overwriteDeltaTable([('a', 3), ('b', 2)],
                                   schema=["key_new", "value_new"],
                                   overwriteSchema='true')

        overwritten = DeltaTable.forPath(self.spark, self.tempFile).toDF()
        self.__checkAnswer(overwritten,
                           [Row(key_new='a', value_new=3), Row(key_new='b', value_new=2)])

        DeltaTable.forPath(self.spark, self.tempFile).restoreToTimestamp(timestampToRestore)

        restored = DeltaTable.forPath(self.spark, self.tempFile).toDF()
        self.__checkAnswer(restored, [Row(key='a', value=1), Row(key='b', value=2)])

        # we cannot test the actual working of restore to timestamp here but we can make sure
        # that the api is being called at least
        def runRestore() -> None:
            DeltaTable.forPath(self.spark, self.tempFile).restoreToTimestamp('05/04/1999')
        self.__intercept(runRestore, "The provided timestamp ('05/04/1999') "
                                     "cannot be converted to a valid timestamp")

    def test_restore_invalid_inputs(self) -> None:
        df = self.spark.createDataFrame([('a', 1), ('b', 2), ('c', 3)], ["key", "value"])
        df.write.format("delta").save(self.tempFile)

        dt = DeltaTable.forPath(self.spark, self.tempFile)

        def runRestoreToTimestamp() -> None:
            dt.restoreToTimestamp(12342323232)  # type: ignore[arg-type]
        self.__intercept(runRestoreToTimestamp,
                         "timestamp needs to be a string but got '<class 'int'>'")

        def runRestoreToVersion() -> None:
            dt.restoreToVersion("0")  # type: ignore[arg-type]
        self.__intercept(runRestoreToVersion,
                         "version needs to be an int but got '<class 'str'>'")

    def test_optimize(self) -> None:
        # write an unoptimized delta table
        df = self.spark.createDataFrame([("a", 1), ("a", 2)], ["key", "value"]).repartition(1)
        df.write.format("delta").save(self.tempFile)
        df = self.spark.createDataFrame([("a", 3), ("a", 4)], ["key", "value"]).repartition(1)
        df.write.format("delta").save(self.tempFile, mode="append")
        df = self.spark.createDataFrame([("b", 1), ("b", 2)], ["key", "value"]).repartition(1)
        df.write.format("delta").save(self.tempFile, mode="append")

        # create DeltaTable
        dt = DeltaTable.forPath(self.spark, self.tempFile)

        # execute bin compaction
        optimizer = dt.optimize()
        res = optimizer.executeCompaction()
        op_params = dt.history().first().operationParameters

        # assertions
        self.assertTrue(isinstance(optimizer, DeltaOptimizeBuilder))
        self.assertTrue(isinstance(res, DataFrame))
        self.assertEqual(1, res.first().metrics.numFilesAdded)
        self.assertEqual(3, res.first().metrics.numFilesRemoved)
        self.assertEqual('[]', op_params['predicate'])

        # test non-partition column
        def optimize() -> None:
            dt.optimize().where("key = 'a'").executeCompaction()
        self.__intercept(optimize,
                         "Predicate references non-partition column 'key'. "
                         "Only the partition columns may be referenced: []")

    def test_optimize_w_partition_filter(self) -> None:
        # write an unoptimized delta table
        df = self.spark.createDataFrame([("a", 1), ("a", 2)], ["key", "value"]).repartition(1)
        df.write.partitionBy("key").format("delta").save(self.tempFile)
        df = self.spark.createDataFrame([("a", 3), ("a", 4)], ["key", "value"]).repartition(1)
        df.write.partitionBy("key").format("delta").save(self.tempFile, mode="append")
        df = self.spark.createDataFrame([("b", 1), ("b", 2)], ["key", "value"]).repartition(1)
        df.write.partitionBy("key").format("delta").save(self.tempFile, mode="append")

        # create DeltaTable
        dt = DeltaTable.forPath(self.spark, self.tempFile)

        # execute bin compaction
        optimizer = dt.optimize().where("key = 'a'")
        res = optimizer.executeCompaction()
        op_params = dt.history().first().operationParameters

        # assertions
        self.assertTrue(isinstance(optimizer, DeltaOptimizeBuilder))
        self.assertTrue(isinstance(res, DataFrame))
        self.assertEqual(1, res.first().metrics.numFilesAdded)
        self.assertEqual(2, res.first().metrics.numFilesRemoved)
        self.assertEqual('["(key = \'a\')"]', op_params['predicate'])

        # test non-partition column
        def optimize() -> None:
            dt.optimize().where("value = 1").executeCompaction()
        self.__intercept(optimize,
                         "Predicate references non-partition column 'value'. "
                         "Only the partition columns may be referenced: [key]")

    def test_optimize_zorder(self) -> None:
        # write an unoptimized delta table
        df = self.spark.createDataFrame([("a", 1), ("a", 2)], ["key", "value"]).repartition(1)
        df.write.format("delta").save(self.tempFile)
        df = self.spark.createDataFrame([("a", 3), ("a", 4)], ["key", "value"]).repartition(1)
        df.write.format("delta").save(self.tempFile, mode="append")
        df = self.spark.createDataFrame([("b", 1), ("b", 2)], ["key", "value"]).repartition(1)
        df.write.format("delta").save(self.tempFile, mode="append")

        # create DeltaTable
        dt = DeltaTable.forPath(self.spark, self.tempFile)

        # execute bin compaction
        optimizer = dt.optimize()
        res = optimizer.executeZOrderBy("key", "value")
        op_params = dt.history().first().operationParameters

        # assertions
        self.assertTrue(isinstance(optimizer, DeltaOptimizeBuilder))
        self.assertTrue(isinstance(res, DataFrame))
        self.assertEqual(1, res.first().metrics.numFilesAdded)
        self.assertEqual(3, res.first().metrics.numFilesRemoved)
        self.assertIsNotNone(res.first().metrics.zOrderStats)
        self.assertEqual('[]', op_params['predicate'])

        # test non-partition column
        def optimize() -> None:
            dt.optimize().where("key = 'a'").executeZOrderBy("key", "value")
        self.__intercept(optimize,
                         "Predicate references non-partition column 'key'. "
                         "Only the partition columns may be referenced: []")

    def test_optimize_zorder_w_partition_filter(self) -> None:
        # write an unoptimized delta table
        df = self.spark.createDataFrame([("a", 1), ("a", 2)], ["key", "value"]).repartition(1)
        df.write.partitionBy("key").format("delta").save(self.tempFile)
        df = self.spark.createDataFrame([("a", 3), ("a", 4)], ["key", "value"]).repartition(1)
        df.write.partitionBy("key").format("delta").save(self.tempFile, mode="append")
        df = self.spark.createDataFrame([("b", 1), ("b", 2)], ["key", "value"]).repartition(1)
        df.write.partitionBy("key").format("delta").save(self.tempFile, mode="append")

        # create DeltaTable
        dt = DeltaTable.forPath(self.spark, self.tempFile)

        # execute bin compaction
        optimizer = dt.optimize().where("key = 'a'")
        res = optimizer.executeZOrderBy("value")
        op_params = dt.history().first().operationParameters

        # assertions
        self.assertTrue(isinstance(optimizer, DeltaOptimizeBuilder))
        self.assertTrue(isinstance(res, DataFrame))
        self.assertEqual(1, res.first().metrics.numFilesAdded)
        self.assertEqual(2, res.first().metrics.numFilesRemoved)
        self.assertIsNotNone(res.first().metrics.zOrderStats)
        self.assertEqual('["(key = \'a\')"]', op_params['predicate'])

        # test non-partition column
        def optimize() -> None:
            dt.optimize().where("value = 1").executeZOrderBy("value")
        self.__intercept(optimize,
                         "Predicate references non-partition column 'value'. "
                         "Only the partition columns may be referenced: [key]")

    def __checkAnswer(self, df: DataFrame,
                      expectedAnswer: List[Any],
                      schema: Union[StructType, List[str]] = ["key", "value"]) -> None:
        if not expectedAnswer:
            self.assertEqual(df.count(), 0)
            return
        expectedDF = self.spark.createDataFrame(expectedAnswer, schema)
        try:
            self.assertEqual(df.count(), expectedDF.count())
            self.assertEqual(len(df.columns), len(expectedDF.columns))
            self.assertEqual([], df.subtract(expectedDF).take(1))
            self.assertEqual([], expectedDF.subtract(df).take(1))
        except AssertionError:
            print("Expected:")
            expectedDF.show()
            print("Found:")
            df.show()
            raise

    def __writeDeltaTable(self, datalist: List[Tuple[Any, Any]]) -> None:
        df = self.spark.createDataFrame(datalist, ["key", "value"])
        df.write.format("delta").save(self.tempFile)

    def __writeAsTable(self, datalist: List[Tuple[Any, Any]], tblName: str) -> None:
        df = self.spark.createDataFrame(datalist, ["key", "value"])
        df.write.format("delta").saveAsTable(tblName)

    def __overwriteDeltaTable(self, datalist: List[Tuple[Any, Any]],
                              schema: Union[StructType, List[str]] = ["key", "value"],
                              overwriteSchema: str = 'false') -> None:
        df = self.spark.createDataFrame(datalist, schema)
        df.write.format("delta") \
            .option('overwriteSchema', overwriteSchema) \
            .mode("overwrite") \
            .save(self.tempFile)

    def __createFile(self, fileName: str, content: Any) -> None:
        with open(os.path.join(self.tempFile, fileName), 'w') as f:
            f.write(content)

    def __checkFileExists(self, fileName: str) -> bool:
        return os.path.exists(os.path.join(self.tempFile, fileName))

    def __intercept(self, func: Callable[[], None], exceptionMsg: str) -> None:
        seenTheRightException = False
        try:
            func()
        except Exception as e:
            if exceptionMsg in str(e):
                seenTheRightException = True
        assert seenTheRightException, ("Did not catch expected Exception:" + exceptionMsg)


if __name__ == "__main__":
    try:
        import xmlrunner
        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports', verbosity=4)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=4)
