package com.github.takezoe.scala.jdbc.validation

import net.sf.jsqlparser.expression.ExpressionVisitorAdapter
import net.sf.jsqlparser.schema.Column
import net.sf.jsqlparser.statement.delete.Delete
import net.sf.jsqlparser.statement.select.SubSelect

import scala.reflect.macros.blackbox

class DeleteValidator(c: blackbox.Context, delete: Delete, schema: Map[String, TableDef]) {

  def validate(): Unit = {
    val tableName = delete.getTable.getName

    schema.get(tableName) match {
      case None => if(schema.nonEmpty){
        c.error(c.enclosingPosition, "Table " + tableName + " does not exist.")
      }
      case Some(tableDef) => {
        val select = new SelectModel()
        val tableModel = new TableModel()
        tableModel.select = Left(tableName)
        select.from += tableModel

        delete.getWhere.accept(new ExpressionVisitorAdapter {
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
