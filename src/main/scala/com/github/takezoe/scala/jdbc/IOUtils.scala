package com.github.takezoe.scala.jdbc

import java.io.{ByteArrayOutputStream, InputStream}
import java.sql.Connection

object IOUtils {

  def closeQuietly(closeable: AutoCloseable): Unit = {
    if(closeable != null){
      try {
        closeable.close()
      } catch {
        case e: Exception => // Ignore
      }
    }
  }

  def rollbackQuietly(conn: Connection): Unit = {
    try {
      conn.rollback()
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

  def using[T <: AutoCloseable, R](closeable: T)(f: T => R): R = {
    try {
      f(closeable)
    } finally {
      closeQuietly(closeable)
    }
  }


  def readStreamAsString(in: InputStream): String = {
    val buf = new Array[Byte](1024 * 8)
    var length = 0
    using(new ByteArrayOutputStream()) { out =>
      while ({ length = in.read(buf); length } != -1) {
        out.write(buf, 0, length)
      }
      new String(out.toByteArray, "UTF-8")
    }
  }

}
