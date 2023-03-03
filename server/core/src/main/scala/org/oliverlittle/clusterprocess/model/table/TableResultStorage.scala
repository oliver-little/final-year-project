package org.oliverlittle.clusterprocess.model.table

import org.oliverlittle.clusterprocess.table_model
import org.oliverlittle.clusterprocess.model.table.field.TableValue

import scala.util.Properties.envOrElse
import java.nio.file.Path
import java.nio.file.Files

trait StoredTableResult[T]:
    def get : TableResult

case class InMemoryTableResult[T](source : T, tableResult : TableResult) extends StoredTableResult[T]:
    val get = tableResult

object ProtobufTableResult:
    val storagePath = Path.of(envOrElse("SPILL_STORAGE_PATH", "temp/"))

case class ProtobufTableResult[T](source : T) extends StoredTableResult[T]:
    val path = ProtobufTableResult.storagePath.resolve(source.getClass.getName + "/" + source.hashCode + ".table")

    def store(tableResult : TableResult) : Unit ={
        val outputStream = Files.newOutputStream(path)
        try {
            tableResult.protobuf.writeTo(outputStream)
        }
        finally {
            outputStream.close
        }
    } 

    def get : TableResult = {
        val inputStream = Files.newInputStream(path)
        try {
            TableResult.fromProtobuf(table_model.TableResult.parseFrom(inputStream))
        }
        finally {
            inputStream.close
        }
    }
