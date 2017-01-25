package com.github.takezoe.scala

import com.github.takezoe.scala.jdbc.SqlTemplate

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
      val sql = sc.parts.mkString("?")
      SqlTemplate(sql, args:_*)
    }
  }

  case class SqlTemplate(sql: String, params: Any*)

  /**
   * Macro version of sql string interpolation.
   * This macro validates the given sql in compile time and returns SqlTemplate as same as string interpolation.
   */
  def sqlc(sql: String): com.github.takezoe.scala.jdbc.SqlTemplate = macro Macros.validateSqlMacro

}

object Macros {

  def validateSqlMacro(c: Context)(sql: c.Expr[String]): c.Expr[com.github.takezoe.scala.jdbc.SqlTemplate] = {
    import c.universe._
    sql.tree match {
      case Literal(x) => x.value match {
        case sql: String => SqlValidator.validateSql(sql, Nil, c)
          val Apply(fun, _) = reify(new SqlTemplate("")).tree
          c.Expr[com.github.takezoe.scala.jdbc.SqlTemplate](Apply.apply(fun, Literal(x) :: Nil))
      }
      case Apply(Select(Apply(Select(Select((_, _)), _), trees), _), args) => {
        val sql = trees.collect { case Literal(x) => x.value.asInstanceOf[String] }.mkString("?")
        SqlValidator.validateSql(sql, args.map(_.tpe.toString), c)
        val Apply(fun, _) = reify(new SqlTemplate("")).tree

//        args.foreach { arg =>
//          println(arg.tpe.getClass)
//        }

        c.Expr[SqlTemplate](Apply.apply(fun, Literal(Constant(sql)) :: args))
      }
      case Select(Apply(Select(a, b), List(Literal(x))), TermName("stripMargin")) => {
        x.value match {
          case s: String =>
            val sql = s.stripMargin
            SqlValidator.validateSql(sql, Nil, c)
            val Apply(fun, _) = reify(new SqlTemplate("")).tree
            c.Expr[SqlTemplate](Apply.apply(fun, Literal(Constant(sql)) :: Nil))
        }
      }
      case Select(Apply(_, List(Apply(Select(Apply(Select(Select((_, _)), _), trees), _), args))), TermName("stripMargin")) => {
        val sql = trees.collect { case Literal(x) => x.value.asInstanceOf[String] }.mkString("?").stripMargin
        SqlValidator.validateSql(sql, args.map(_.tpe.toString), c)
        val Apply(fun, _) = reify(new SqlTemplate("")).tree
        c.Expr[SqlTemplate](Apply.apply(fun, Literal(Constant(sql)) :: args))
      }
    }
  }

}
