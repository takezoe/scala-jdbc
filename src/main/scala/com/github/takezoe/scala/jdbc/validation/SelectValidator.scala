package com.github.takezoe.scala.jdbc.validation

import net.sf.jsqlparser.statement.select.Select

import scala.reflect.macros.blackbox

class SelectValidator(c: blackbox.Context, select: Select, schema: Map[String, TableDef]) {

  def validate(): Unit = {
    val visitor = new SelectVisitor(c)
    select.getSelectBody.accept(visitor)

    visitor.select.validate(c, schema)
  }

}
