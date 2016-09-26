package com.github.takezoe.scala.jdbc

import java.sql.Connection

object JdbcUtils {

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

}
