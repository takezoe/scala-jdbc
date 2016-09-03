package com.github.takezoe.scala

import net.sf.jsqlparser.JSQLParserException
import net.sf.jsqlparser.parser.CCJSqlParserUtil
import net.sf.jsqlparser.statement.StatementVisitorAdapter

import scala.language.experimental.macros
import scala.reflect.macros.blackbox.Context
import com.github.takezoe.scala.jdbc.validation._

package object jdbc {

  /**
   * Implicit conversion to convert a raw string with no parameter to SqlTemplate
   */
  implicit def String2SqlTemplate(sql: String) = SqlTemplate(sql)

  /**
   * String interpolation to convert a variable-embedded SQL to SqlTemplate
   */
  implicit class SqlStringInterpolation(val sc: StringContext) extends AnyVal {
    def sql(args: Any*): SqlTemplate = {
      val sql = sc.parts.mkString
      println(sql)
      val parse = CCJSqlParserUtil.parse(sql)
      SqlTemplate(sql, args.toSeq)
    }
  }

  case class SqlTemplate(sql: String, params: Any*)

  def sqlc(sql: String): String = macro Macros.sqlMacro

}

object Macros {

  def sqlMacro(c: Context)(sql: c.Expr[String]): c.Expr[String] = {
    import c.universe._
    sql.tree match {
      case Literal(x) => x.value match {
        case s: String => {
          try {
            val parse = CCJSqlParserUtil.parse(s)
            parse.accept(new StatementVisitorAdapter {
              override def visit(select: net.sf.jsqlparser.statement.select.Select): Unit = {
                val visitor = new SelectVisitor(c)
                select.getSelectBody.accept(visitor)
              }
            })
          } catch {
            case e: JSQLParserException => c.error(c.enclosingPosition, e.getCause.getMessage)
          }
        }
      }
    }
    c.Expr[String](q"$sql")
  }

}
