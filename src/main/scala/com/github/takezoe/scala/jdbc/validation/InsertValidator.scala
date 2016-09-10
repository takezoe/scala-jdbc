package com.github.takezoe.scala.jdbc.validation

import net.sf.jsqlparser.statement.insert.Insert

import scala.collection.JavaConverters._
import scala.reflect.macros.blackbox

class InsertValidator(c: blackbox.Context, insert: Insert, schema: Map[String, TableDef]) {

  def validate(): Unit = {
    val tableName = insert.getTable.getName

    schema.get(tableName) match {
      case None => c.error(c.enclosingPosition, "Table " + tableName + " does not exist.")
      case Some(tableDef) => insert.getColumns.asScala.foreach { column =>
        if(!tableDef.columns.exists(_.name == column.getColumnName)){
          c.error(c.enclosingPosition, "Column " + column.getColumnName + " does not exist in " + tableDef.name + ".")
        }
      }
    }

    if(insert.getSelect != null){
      val visitor = new SelectVisitor(c)
      insert.getSelect.getSelectBody.accept(visitor)
      visitor.select.validate(c, schema)
    }
  }

}
