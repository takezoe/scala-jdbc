package com.github.takezoe.scala.jdbc.validation

import net.sf.jsqlparser.JSQLParserException
import net.sf.jsqlparser.parser.CCJSqlParserUtil
import net.sf.jsqlparser.statement.StatementVisitorAdapter
import net.sf.jsqlparser.statement.delete.Delete
import net.sf.jsqlparser.statement.insert.Insert
import net.sf.jsqlparser.statement.update.Update

import scala.reflect.macros.blackbox.Context

object SqlValidator {

  def validateSql(sql: String, c: Context): Unit = {
    val schema = SchemaDef.load()
    try {
      val parse = CCJSqlParserUtil.parse(sql)
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
