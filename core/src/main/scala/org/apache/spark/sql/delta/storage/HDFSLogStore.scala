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

package org.apache.spark.sql.delta.storage

import java.io.{IOException, _}
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.FileAlreadyExistsException
import java.util.{EnumSet, UUID}
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._
import scala.collection.mutable.{HashMap, Map}
import scala.util.control.NonFatal

import org.apache.commons.io.IOUtils
import org.apache.spark.sql.delta.DeltaErrors
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.fs.CreateFlag.CREATE
import org.apache.hadoop.fs.Options.{ChecksumOpt, CreateOpts}

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging

/**
 * The [[LogStore]] implementation for HDFS, which uses Hadoop [[FileContext]] API's to
 * provide the necessary atomic and durability guarantees:
 *
 * 1. Atomic visibility of files: `FileContext.rename` is used write files which is atomic for HDFS.
 *
 * 2. Consistent file listing: HDFS file listing is consistent.
 */
class HDFSLogStore(sparkConf: SparkConf, defaultHadoopConf: Configuration)
  extends HadoopFileSystemLogStore(sparkConf, defaultHadoopConf) with Logging{

  // Cache the file context based on the scheme and authority. A single log store should only work
  // with one file system, but we don't have access to the log path in the constructor
  private val fileContextCache: Map[(String, String), FileContext] = new HashMap

  protected def getFileContext(path: Path, write: Boolean = false): Option[FileContext] = {
    try {
      val qualifiedPath = resolvePathOnPhysicalStorage(path)
      val scheme = qualifiedPath.toUri.getScheme()
      assert(scheme != null, "path must be fully qualified")
      val authority = qualifiedPath.toUri.getAuthority()
      Some(fileContextCache.getOrElseUpdate((scheme, authority),
        FileContext.getFileContext(qualifiedPath.toUri, getHadoopConfiguration)))
    } catch {
    case e: IOException if e.getMessage.contains(noAbstractFileSystemExceptionMessage) =>
      if (write) {
        val newException = DeltaErrors.incorrectLogStoreImplementationException(sparkConf, e)
        logError(newException.getMessage, newException.getCause)
        throw newException
      }
      None
    }
  }

  val noAbstractFileSystemExceptionMessage = "No AbstractFileSystem"

  override def read(path: Path): Seq[String] = {
    getFileContext(path) match {
      case Some(fc) =>
        val stream = fc.open(path)
        try {
          val reader = new BufferedReader(new InputStreamReader(stream, UTF_8))
          IOUtils.readLines(reader).asScala.map(_.trim)
        } finally {
          stream.close()
        }
      case None => super.read(path)
    }
  }

  override def listFrom(path: Path): Iterator[FileStatus] = {
    getFileContext(path) match {
      case Some(fc) =>
        if (!fc.util.exists(path.getParent)) {
          throw new FileNotFoundException(s"No such file or directory: ${path.getParent}")
        }
        val files = fc.util.listStatus(path.getParent)
        files.filter(_.getPath.getName >= path.getName).sortBy(_.getPath.getName).iterator
      case None => super.listFrom(path)
    }
  }

  def write(path: Path, actions: Iterator[String], overwrite: Boolean = false): Unit = {
    val isLocalFs = path.getFileSystem(getHadoopConfiguration).isInstanceOf[RawLocalFileSystem]
    if (isLocalFs) {
      // We need to add `synchronized` for RawLocalFileSystem as its rename will not throw an
      // exception when the target file exists. Hence we must make sure `exists + rename` in
      // `writeInternal` for RawLocalFileSystem is atomic in our tests.
      synchronized {
        writeInternal(path, actions, overwrite)
      }
    } else {
      // rename is atomic and also will fail when the target file exists. Not need to add the extra
      // `synchronized`.
      writeInternal(path, actions, overwrite)
    }
  }

  private def writeInternal(path: Path, actions: Iterator[String], overwrite: Boolean): Unit = {
    val fc = getFileContext(path, true).get
    if (!overwrite && fc.util.exists(path)) {
      // This is needed for the tests to throw error with local file system
      throw new FileAlreadyExistsException(path.toString)
    }

    val tempPath = createTempPath(path)
    var streamClosed = false // This flag is to avoid double close
    var renameDone = false // This flag is to save the delete operation in most of cases.
    val stream = fc.create(
      tempPath, EnumSet.of(CREATE), CreateOpts.checksumParam(ChecksumOpt.createDisabled()))

    try {
      actions.map(_ + "\n").map(_.getBytes(UTF_8)).foreach(stream.write)
      stream.close()
      streamClosed = true
      try {
        val renameOpt = if (overwrite) Options.Rename.OVERWRITE else Options.Rename.NONE
        fc.rename(tempPath, path, renameOpt)
        renameDone = true
        // TODO: this is a workaround of HADOOP-16255 - remove this when HADOOP-16255 is resolved
        tryRemoveCrcFile(fc, tempPath)
      } catch {
        case e: org.apache.hadoop.fs.FileAlreadyExistsException =>
          throw new FileAlreadyExistsException(path.toString)
      }
    } finally {
      if (!streamClosed) {
        stream.close()
      }
      if (!renameDone) {
        fc.delete(tempPath, false)
      }
    }
  }

  private def tryRemoveCrcFile(fc: FileContext, path: Path): Unit = {
    try {
      val checksumFile = new Path(path.getParent, s".${path.getName}.crc")
      if (fc.util.exists(checksumFile)) {
        // checksum file exists, deleting it
        fc.delete(checksumFile, true)
      }
    } catch {
      case NonFatal(_) => // ignore, we are removing crc file as "best-effort"
    }
  }

  override def isPartialWriteVisible(path: Path): Boolean = true
}
