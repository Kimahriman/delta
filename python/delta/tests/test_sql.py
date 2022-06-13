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
import tempfile
import os
from typing import List, Any

from pyspark.sql import DataFrame, SparkSession
import pytest

from chispa import assert_df_equality

@pytest.fixture(scope='function')
def table_path(spark_session: SparkSession, tmp_path: Path):
    df = spark_session.createDataFrame([('a', 1), ('b', 2), ('c', 3)], ["key", "value"])
    path = tmp_path.as_posix()
    df.write.format("delta").save(path)
    df.write.mode("overwrite").format("delta").save(path)
    return path

@pytest.fixture(scope='function')
def database(spark_session: SparkSession, tmp_path: Path, request: pytest.FixtureRequest):
    path = tmp_path.as_posix()
    db_name = 'test_database'
    spark_session.sql(f"CREATE DATABASE {db_name} LOCATION '{path}'")
    spark_session.catalog.setCurrentDatabase(db_name)
    yield db_name
    spark_session.sql(f'DROP DATABASE {db_name} CASCADE')


def test_vacuum(spark_session: SparkSession, table_path: str) -> None:
    spark_session.sql("set spark.databricks.delta.retentionDurationCheck.enabled = false")
    try:
        deleted_files = spark_session.sql(f"VACUUM '{table_path}' RETAIN 0 HOURS").collect()
        # Verify `VACUUM` did delete some data files
        assert table_path in deleted_files[0][0]
    finally:
        spark_session.sql("set spark.databricks.delta.retentionDurationCheck.enabled = true")

def test_describe_history(spark_session: SparkSession, table_path: str) -> None:
    assert len(spark_session.sql(f"desc history delta.`{table_path}`").collect()) > 0

def test_generate(spark_session: SparkSession, tmp_path: Path) -> None:
    # create a delta table
    numFiles = 10
    path = tmp_path.as_posix()
    spark_session.range(100).repartition(numFiles).write.format("delta").save(path)

    # Generate the symlink format manifest
    spark_session.sql(f"GENERATE SYMLINK_FORMAT_MANIFEST FOR TABLE delta.`{path}`")

    # check the contents of the manifest
    # NOTE: this is not a correctness test, we are testing correctness in the scala suite
    manifestPath = os.path.join(path, "_symlink_format_manifest", "manifest")
    files = []
    with open(manifestPath) as f:
        files = f.readlines()

    # the number of files we write should equal the number of lines in the manifest
    assert len(files) == numFiles

def test_convert(spark_session: SparkSession, tmp_path: Path) -> None:
    df = spark_session.createDataFrame([('a', 1), ('b', 2), ('c', 3)], ["key", "value"])
    path = tmp_path.as_posix()
    temp_file2 = os.path.join(path, "delta_sql_test2")
    temp_file3 = os.path.join(path, "delta_sql_test3")

    df.write.format("parquet").save(temp_file2)
    spark_session.sql(f"CONVERT TO DELTA parquet.`{temp_file2}`")
    __checkAnswer(
        spark_session,
        spark_session.read.format("delta").load(temp_file2),
        [('a', 1), ('b', 2), ('c', 3)])

    # test if convert to delta with partition columns work
    df.write.partitionBy("value").format("parquet").save(temp_file3)
    spark_session.sql(f"CONVERT TO DELTA parquet.`{temp_file3}` PARTITIONED BY (value LONG)")
    __checkAnswer(
        spark_session,
        spark_session.read.format("delta").load(temp_file3),
        [('a', 1), ('b', 2), ('c', 3)])

def test_ddls(spark_session: SparkSession, database: str) -> None:
    table = "deltaTable"
    table2 = "deltaTable2"
    def read_table() -> DataFrame:
        return spark_session.sql(f"SELECT * FROM {table}")

    spark_session.sql(f"CREATE TABLE {table} (a LONG, b String NOT NULL) USING delta")
    assert read_table().count() == 0

    __checkAnswer(
        spark_session,
        spark_session.sql(f"DESCRIBE TABLE {table}").select("col_name", "data_type"),
        [("a", "bigint"), ("b", "string"), ("", ""), ("# Partitioning", ""),
            ("Not partitioned", "")],
        schema=["col_name", "data_type"])

    spark_session.sql(f"ALTER TABLE {table} CHANGE COLUMN a a LONG AFTER b")
    assert ["b", "a"] == [f.name for f in read_table().schema.fields]

    spark_session.sql(f"ALTER TABLE {table} ALTER COLUMN b DROP NOT NULL")
    assert True in [f.nullable for f in read_table().schema.fields if f.name == "b"]

    spark_session.sql(f"ALTER TABLE {table} ADD COLUMNS (x LONG)")
    assert "x" in [f.name for f in read_table().schema.fields]

    spark_session.sql(f"ALTER TABLE {table} SET TBLPROPERTIES ('k' = 'v')")
    __checkAnswer(spark_session,
                        spark_session.sql(f"SHOW TBLPROPERTIES {table}"),
                        [('Type', 'MANAGED'),
                        ('k', 'v'),
                        ('delta.minReaderVersion', '1'),
                        ('delta.minWriterVersion', '2')])

    spark_session.sql(f"ALTER TABLE {table} UNSET TBLPROPERTIES ('k')")
    __checkAnswer(spark_session,
                        spark_session.sql(f"SHOW TBLPROPERTIES {table}"),
                        [('Type', 'MANAGED'),
                        ('delta.minReaderVersion', '1'),
                        ('delta.minWriterVersion', '2')])

    spark_session.sql(f"ALTER TABLE {table} RENAME TO {table2}")
    assert spark_session.sql(f"SELECT * FROM {table2}").count() == 0

    test_dir = os.path.join(tempfile.mkdtemp(), table2)
    spark_session.createDataFrame([("", 0, 0)], ["b", "a", "x"]) \
        .write.format("delta").save(test_dir)

    spark_session.sql(f"ALTER TABLE {table2} SET LOCATION '{test_dir}'")
    assert spark_session.sql(f"SELECT * FROM {table2}").count() == 1

def __checkAnswer(spark: SparkSession, df: DataFrame,
                      expectedAnswer: List[Any],
                      schema: List[str] = ["key", "value"]) -> None:
    if not expectedAnswer:
        assert df.count() == 0
        return

    expectedDF = spark.createDataFrame(expectedAnswer, schema)
    assert_df_equality(df.select('*'), expectedDF, ignore_nullable=True, ignore_row_order=True)

