package com.stratio.sparta

import com.azure.storage.blob.BlobServiceClientBuilder
import com.azure.storage.blob.models.BlobItem
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.crossdata.XDSession

import scala.util.{Failure, Success, Try}

object funciones {

  val conf: Configuration = new Configuration
  val fs: FileSystem = FileSystem.get(conf)

  def downloadFiles( xdSession: XDSession, properties: Map[String, String]): DataFrame =  {

    val filesystem: String = properties("filesystem")
    val accountName: String = properties("accountName")
    val file: String = properties("file")
    val endPoint: String = properties("endPoint")
    val token: String = properties("token")
    val contenedor: String = properties("contenedor")
    val urlSalida: String = properties("urlSalida")
    val nombreArchivo: String = properties("nombreArchivo")

    val blobServiceClient = new BlobServiceClientBuilder().endpoint(endPoint).sasToken(token).buildClient
    val blobContainerClient = blobServiceClient.getBlobContainerClient(contenedor)

    val blobs = blobContainerClient.listBlobs().iterator()

    while (blobs.hasNext) {
      val blob: BlobItem = blobs.next()
      val dir = blob.getName.split("/")
      System.out.println("Archivo: " + blob.getName)
      if (blob.getName.startsWith(nombreArchivo) && blob.getName.endsWith(".parquet")) {
        Try(fs.create(new Path(urlSalida + dir(dir.size-1))))
        match {
          case Failure(exception) =>
          case Success(value) =>
            blobContainerClient.getBlobClient(blob.getName).download(value)
//            value.write(s.toBytes)
            value.close()
        }
      }
    }

    xdSession.read.parquet("abfss://"+filesystem+"@"+accountName+".dfs.core.windows.net/"+file)

  }
}
