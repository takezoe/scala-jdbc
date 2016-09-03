package com.github.takezoe.scala.jdbc.validation

import scala.collection.mutable.ListBuffer
import scala.reflect.macros.blackbox.Context

class TableModel {
  var name: String = null
  var alias: String = null
  var select: SelectModel = null

  override def toString(): String = {
    s"Select(name: $name, alias: $alias, select: $select)"
  }
}

class SelectModel {
  var from = new ListBuffer[TableModel]
  val columns = new ListBuffer[ColumnModel]
  val orderBy = new ListBuffer[ColumnModel]
  val where = new ListBuffer[ColumnModel]

  override def toString(): String = {
    s"Select(from: $from, columns: $columns, orderBy; $orderBy, where: $where)"
  }

  def validate(c: Context): Unit = {
    from.foreach { table =>
      if(table.select != null){
        table.select.validate(c)
      }
    }
    (columns ++ where ++ orderBy).foreach { column =>
      println(column)
      val table = if (column.table != null){
        from.find(x => x.name == column.table || x.alias == column.table)
      } else {
        from.headOption
      }
      table.map { table =>
        if(table.name != null){
          // TODO Check with table definition which would be given from user
        } else {
          if (!table.select.columns.exists(x => x.name == column.name || x.alias == column.name)) {
            c.error(c.enclosingPosition, "[ERROR]Column " + column + " does not exist!")
          }
        }
      }.getOrElse {
        c.error(c.enclosingPosition, "[ERROR]Table " + column.table + " does not exist!")
      }
    }
  }
}

class ColumnModel {
  var name: String = null
  var table: String = null
  var alias: String = null

  override def toString(): String = {
    s"Column(table: $table, name: $name, alias: $alias)"
  }
}

