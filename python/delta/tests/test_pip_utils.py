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

from pyspark.sql import SparkSession
import delta


def test_maven_jar_loaded(spark_session: SparkSession, tmp_path: Path) -> None:
    # Read and write Delta table to check that the maven jars are loaded and Delta works.
    spark_session.range(0, 5).write.format("delta").save(tmp_path.as_uri())
    spark_session.read.format("delta").load(tmp_path.as_uri())

def test_configure_spark() -> None:
    import importlib_metadata
    scala_version = "2.12"
    delta_version = importlib_metadata.version("delta_spark")

    builder = delta.configure_spark_with_delta_pip(SparkSession.builder, ['extra:package'])
    packages = builder._options.get("spark.jars.packages").split(",")
    
    assert f"io.delta:delta-core_{scala_version}:{delta_version}" in packages
    assert "extra:package" in packages
