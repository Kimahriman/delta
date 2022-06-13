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

from typing import Any, Callable, TYPE_CHECKING

import pytest

import delta.exceptions as exceptions

from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException, IllegalArgumentException

if TYPE_CHECKING:
    from py4j.java_gateway import JVMView  # type: ignore[import]

@pytest.fixture(scope='session')
def jvm(spark_session: SparkSession) -> "JVMView":
    return spark_session.sparkContext._jvm

def _raise_concurrent_exception(jvm: "JVMView", exception_type: Callable[[Any], Any]) -> None:
    e = exception_type("")
    jvm.scala.util.Failure(e).get()

def test_capture_concurrent_write_exception(jvm: "JVMView") -> None:
    e = jvm.io.delta.exceptions.ConcurrentWriteException
    pytest.raises(exceptions.ConcurrentWriteException,
                        lambda: _raise_concurrent_exception(jvm, e))

def test_capture_metadata_changed_exception(jvm: "JVMView") -> None:
    e = jvm.io.delta.exceptions.MetadataChangedException
    pytest.raises(exceptions.MetadataChangedException,
                        lambda: _raise_concurrent_exception(jvm, e))

def test_capture_protocol_changed_exception(jvm: "JVMView") -> None:
    e = jvm.io.delta.exceptions.ProtocolChangedException
    pytest.raises(exceptions.ProtocolChangedException,
                        lambda: _raise_concurrent_exception(jvm, e))

def test_capture_concurrent_append_exception(jvm: "JVMView") -> None:
    e = jvm.io.delta.exceptions.ConcurrentAppendException
    pytest.raises(exceptions.ConcurrentAppendException,
                        lambda: _raise_concurrent_exception(jvm, e))

def test_capture_concurrent_delete_read_exception(jvm: "JVMView") -> None:
    e = jvm.io.delta.exceptions.ConcurrentDeleteReadException
    pytest.raises(exceptions.ConcurrentDeleteReadException,
                        lambda: _raise_concurrent_exception(jvm, e))

def test_capture_concurrent_delete_delete_exception(jvm: "JVMView") -> None:
    e = jvm.io.delta.exceptions.ConcurrentDeleteDeleteException
    pytest.raises(exceptions.ConcurrentDeleteDeleteException,
                        lambda: _raise_concurrent_exception(jvm, e))

def test_capture_concurrent_transaction_exception(jvm: "JVMView") -> None:
    e = jvm.io.delta.exceptions.ConcurrentTransactionException
    pytest.raises(exceptions.ConcurrentTransactionException,
                        lambda: _raise_concurrent_exception(jvm, e))

def test_capture_delta_analysis_exception(jvm: "JVMView") -> None:
    e = jvm.org.apache.spark.sql.delta.DeltaErrors.invalidColumnName
    pytest.raises(AnalysisException,
                        lambda: jvm.scala.util.Failure(e("invalid")).get())

def test_capture_delta_illegal_argument_exception(jvm: "JVMView") -> None:
    e = jvm.org.apache.spark.sql.delta.DeltaErrors
    method = e.throwDeltaIllegalArgumentException
    pytest.raises(IllegalArgumentException,
                        lambda: jvm.scala.util.Failure(method()).get())
