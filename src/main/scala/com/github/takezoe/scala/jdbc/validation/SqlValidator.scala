package com.github.takezoe.scala.jdbc.validation

import java.sql.{Date, DriverManager, Time, Timestamp}

import net.sf.jsqlparser.JSQLParserException
import net.sf.jsqlparser.parser.CCJSqlParserUtil
import net.sf.jsqlparser.statement.StatementVisitorAdapter
import net.sf.jsqlparser.statement.delete.Delete
import net.sf.jsqlparser.statement.insert.Insert
import net.sf.jsqlparser.statement.update.Update

import scala.reflect.macros.blackbox.Context

import com.github.takezoe.scala.jdbc.IOUtils._
import com.github.takezoe.scala.jdbc.TypeMapper

object SqlValidator {

  val typeMapper = new TypeMapper() // TODO It should be replaceable.

  def validateSql(sql: String, types: Seq[String], c: Context): Unit = {
    SchemaDef.load() match {
      case None => {
        try {
          CCJSqlParserUtil.parse(sql)
        } catch {
          case e: JSQLParserException => c.error(c.enclosingPosition, e.getCause.getMessage)
        }
      }
      case Some(SchemaDef(_, Some(connection))) => {
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
      case Some(schemaDef) => {
        try {
          val parse = CCJSqlParserUtil.parse(sql)
          val schema = schemaDef.toMap
          parse.accept(new StatementVisitorAdapter {
            override def visit(select: net.sf.jsqlparser.statement.select.Select): Unit = {
              new SelectValidator(c, select, schema).validate()
            }
            override def visit(insert: Insert): Unit = {
              new InsertValidator(c, insert, schema).validate()
            }
            override def visit(update: Update): Unit = {
              new UpdateValidator(c, update, schema).validate()
            }
            override def visit(delete: Delete): Unit = {
              new DeleteValidator(c, delete, schema).validate()
            }
          })
        } catch {
          case e: JSQLParserException => c.error(c.enclosingPosition, e.getCause.getMessage)
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
