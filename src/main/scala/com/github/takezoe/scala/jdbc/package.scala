package com.github.takezoe.scala

import com.github.takezoe.scala.jdbc.SqlTemplate

import scala.language.experimental.macros
import scala.reflect.macros.blackbox.Context

import scala.language.implicitConversions

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
}
