package com.github.takezoe.scala

import com.github.takezoe.scala.jdbc.SqlTemplate
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
      SqlTemplate(sql, args.toSeq)
    }
  }

  case class SqlTemplate(sql: String, params: Any*)

  def sqlc(sql: String): com.github.takezoe.scala.jdbc.SqlTemplate = macro Macros.sqlMacro

}

object Macros {

  def sqlMacro(c: Context)(sql: c.Expr[String]): c.Expr[com.github.takezoe.scala.jdbc.SqlTemplate] = {
    import c.universe._
    sql.tree match {
      case Literal(x) => x.value match {
        case sql: String => validateSql(sql, c)
          val Apply(fun, _) = reify(new SqlTemplate("")).tree
          c.Expr[com.github.takezoe.scala.jdbc.SqlTemplate](Apply.apply(fun, Literal(x) :: Nil))
      }
      case Apply(Select(Apply(Select(Select((_, _)), _), trees), _), args) => {
        val sql = trees.collect { case Literal(x) => x.value.asInstanceOf[String] }.mkString("?")
        validateSql(sql, c)
        val Apply(fun, _) = reify(new SqlTemplate("")).tree
        c.Expr[SqlTemplate](Apply.apply(fun, Literal(Constant(sql)) :: args))
      }
      case Select(Apply(_, List(Apply(Select(Apply(Select(Select((_, _)), _), trees), _), args))), TermName("stripMargin")) => {
        val sql = trees.collect { case Literal(x) => x.value.asInstanceOf[String] }.mkString("?").stripMargin
        validateSql(sql, c)
        val Apply(fun, _) = reify(new SqlTemplate("")).tree
        c.Expr[SqlTemplate](Apply.apply(fun, Literal(Constant(sql)) :: args))
      }
    }
  }

  private def validateSql(sql: String, c: Context): Unit = {
    try {
      val parse = CCJSqlParserUtil.parse(sql)
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
