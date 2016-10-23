package com.github.takezoe.scala.jdbc.validation

import java.sql.{Date, DriverManager, Time, Timestamp}

import scala.reflect.macros.blackbox.Context
import com.github.takezoe.scala.jdbc.IOUtils._
import com.github.takezoe.scala.jdbc.TypeMapper
import net.sf.jsqlparser.JSQLParserException
import net.sf.jsqlparser.parser.CCJSqlParserUtil

object SqlValidator {

  val typeMapper = new TypeMapper() // TODO It should be replaceable.

  def validateSql(sql: String, types: Seq[String], c: Context): Unit = {
    ConnectionDef.load() match {
      case None => {
        try {
          CCJSqlParserUtil.parse(sql)
        } catch {
          case e: JSQLParserException => c.error(c.enclosingPosition, e.getCause.getMessage)
        }
      }
      case Some(connection) => {
        Class.forName(connection.driver)
        val conn = DriverManager.getConnection(connection.url, connection.user, connection.password)
        try {
          conn.setAutoCommit(false)
          using(conn.prepareStatement(adjustSql(sql))){ stmt =>
            try {
              types.zipWithIndex.foreach { case (t, i) =>
                typeMapper.set(stmt, i + 1, getTestValue(t))
              }
              stmt.execute()
            } catch {
              case e: Exception => c.error(c.enclosingPosition, e.toString)
            }
          }
        } finally {
          rollbackQuietly(conn)
          closeQuietly(conn)
        }
      }
    }
  }

  private def adjustSql(sql: String): String = {
    if(sql.trim.toUpperCase.startsWith("SELECT")){
      sql + " LIMIT 0"
    } else {
      sql
    }
  }

  // TODO Move to TypeMapper?
  private def getTestValue(t: String): Any = {
    t match {
      case "Int"                => 0
      case "Long"               => 0L
      case "Double"             => 0D
      case "Short"              => 0:Short
      case "Float"              => 0F
      case "java.sql.Timestamp" => new Timestamp(System.currentTimeMillis)
      case "java.sql.Date"      => new Date(System.currentTimeMillis)
      case "java.sql.Time"      => new Time(System.currentTimeMillis)
      case "String"             => "-"
    }
  }

}
