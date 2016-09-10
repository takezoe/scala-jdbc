package com.github.takezoe.scala.jdbc.validation

import net.sf.jsqlparser.expression.ExpressionVisitorAdapter
import net.sf.jsqlparser.schema.Column
import net.sf.jsqlparser.statement.select.SubSelect
import net.sf.jsqlparser.statement.update.Update

import scala.collection.JavaConverters._
import scala.reflect.macros.blackbox

class UpdateValidator(c: blackbox.Context, update: Update, schema: Map[String, TableDef]) {

  def validate(): Unit = {
    val tableName = update.getTables.asScala.head.getName

    schema.get(tableName) match {
      case None => c.error(c.enclosingPosition, "Table " + tableName + " does not exist.")
      case Some(tableDef) => update.getColumns.asScala.foreach { column =>
        if(!tableDef.columns.exists(_.name == column.getColumnName)){
          c.error(c.enclosingPosition, "Column " + column.getColumnName + " does not exist in " + tableDef.name + ".")
        }

        val select = new SelectModel()
        val tableModel = new TableModel()
        tableModel.select = Left(tableName)
        select.from += tableModel

        update.getWhere.accept(new ExpressionVisitorAdapter {
          override def visit(column: Column): Unit = {
            val c = new ColumnModel()
            c.name = column.getColumnName
            c.table = Option(tableName)
            select.where += c
          }

          override def visit(subSelect: SubSelect): Unit = {
            val visitor = new SelectVisitor(c)
            subSelect.getSelectBody.accept(visitor)
            select.others += visitor.select
          }
        })

        select.validate(c, schema)
      }
    }
  }

}
