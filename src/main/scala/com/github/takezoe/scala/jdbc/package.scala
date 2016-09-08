package com.github.takezoe.scala

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.github.takezoe.scala.jdbc.SqlTemplate
import net.sf.jsqlparser.JSQLParserException
import net.sf.jsqlparser.parser.CCJSqlParserUtil
import net.sf.jsqlparser.statement.StatementVisitorAdapter

import scala.language.experimental.macros
import scala.reflect.macros.blackbox.Context
import com.github.takezoe.scala.jdbc.validation._

import scala.reflect.ClassTag
import better.files._
import java.io.{File => JFile}

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

  private val mapper = new ObjectMapper()
  mapper.enable(DeserializationFeature.UNWRAP_SINGLE_VALUE_ARRAYS)
  mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
  mapper.registerModule(DefaultScalaModule)

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
    val file = File("schema.json")
    val schema: Map[String, TableDef] = if(file.exists){
      val json = file.contentAsString
      val schema = mapper.readValue(json, classOf[SchemaDef])
      schema.tables.map { t => t.name -> t }.toMap
    } else {
      Map.empty
    }
    try {
      val parse = CCJSqlParserUtil.parse(sql)
      parse.accept(new StatementVisitorAdapter {
        override def visit(select: net.sf.jsqlparser.statement.select.Select): Unit = {
          val visitor = new SelectVisitor(c)
          select.getSelectBody.accept(visitor)

          visitor.select.validate(c, schema)
        }
      })
    } catch {
      case e: JSQLParserException => c.error(c.enclosingPosition, e.getCause.getMessage)
    }
  }

}
