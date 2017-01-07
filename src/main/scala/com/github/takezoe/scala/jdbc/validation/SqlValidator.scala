package com.github.takezoe.scala.jdbc.validation

import scala.reflect.macros.blackbox.Context
import net.sf.jsqlparser.JSQLParserException
import net.sf.jsqlparser.parser.CCJSqlParserUtil

object SqlValidator {

  def validateSql(sql: String, types: Seq[String], c: Context): Unit = {
    try {
      CCJSqlParserUtil.parse(sql)
    } catch {
      case e: JSQLParserException => c.error(c.enclosingPosition, e.getCause.getMessage)
    }
  }

}
