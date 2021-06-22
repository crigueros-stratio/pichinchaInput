package com.stratio

import com.azure.storage.blob.BlobServiceClientBuilder
import com.azure.storage.blob.models.BlobItem
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

import scala.util.{Failure, Success, Try}

import scala.collection.JavaConversions._

object Test2 {


  val sparkSession: SparkSession = SparkSession.builder()
    .appName("app")
    .master("local[*]")
    .getOrCreate()

  //  val filesystem: String = "riesgo"
  //  val accountName: String = "extstratio"
  //  val file: String = "cob.VIS_CLE_HISTORICO_RIESGOS_MENSUAL"


  val endPoint: String = "https://extstratio.blob.core.windows.net"
  val token: String = "?sv=2020-02-10&st=2021-06-03T21%3A30%3A44Z&se=2021-06-30T21%3A30%3A00Z&sr=c&sp=racwdlme&sig=rzE5dWaRjrjt%2FHx43mbiZCnPQ5%2B5288mFdImjSH0SDk%3D"
  val contenedor: String = "riesgo"
  val urlSalida: String = "/home/stratio7/kmilo/Stratio/PICHINCHA/codigos/archivos/"
  val nombreArchivo: String = "rie.varScoreCbz_gold"

  val conf: Configuration = new Configuration
  val fs: FileSystem = FileSystem.get(conf)

  def main(args: Array[String]): Unit = {

    val blobServiceClient = new BlobServiceClientBuilder().endpoint(endPoint).sasToken(token).buildClient
    val blobContainerClient = blobServiceClient.getBlobContainerClient(contenedor)
    System.out.println(blobContainerClient.listBlobs().size)

    val blobs = blobContainerClient.listBlobs()
    for (blob <- blobs) {
      val dir = blob.getName.split("/")
      System.out.println("Archivo: " + blob.getName)
      if (blob.getName.startsWith(nombreArchivo) && blob.getName.endsWith(".parquet")) {
        Try(fs.create(new Path(urlSalida + dir(dir.size - 1))))
        match {
          case Failure(exception) =>

          case Success(value) =>
            blobContainerClient.getBlobClient(blob.getName).download(value)
            value.close()
        }
      }
    }
  }
}
