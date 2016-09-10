package com.github.takezoe.scala.jdbc.validation

import scala.collection.mutable.ListBuffer
import scala.reflect.macros.blackbox.Context

class TableModel {
  var select: Either[String, SelectModel] = null
  var alias: Option[String] = None

  override def toString(): String = {
    s"Select(select: $select, alias: $alias)"
  }
}

class SelectModel {
  val from = new ListBuffer[TableModel]
  val columns = new ListBuffer[ColumnModel]
  val orderBy = new ListBuffer[ColumnModel]
  val where = new ListBuffer[ColumnModel]
  val others = new ListBuffer[SelectModel]

  override def toString(): String = {
    s"Select(from: $from, columns: $columns, orderBy; $orderBy, where: $where)"
  }

  def validate(c: Context, schema: Map[String, TableDef]): Unit = {
    from.foreach { table =>
      table.select.right.foreach(_.validate(c, schema))
    }
    (columns ++ where ++ orderBy).foreach { column =>
      val table = column.table.map { t =>
        from.find(x => x.select == Left(t) || x.alias == Some(t))
      }.getOrElse {
        from.headOption
      }

      table.map { t =>
        t.select match {
          case Left(name) => {
            schema.get(name) match {
              case Some(t) => if(!t.columns.exists(_.name == column.name)){
                c.error(c.enclosingPosition, "Column " + column.fullName + " does not exist.")
              }
              case None => c.error(c.enclosingPosition, "Table " + name + " does not exist.")
            }
          }
          case Right(select) => {
            if (!select.columns.exists { x => x.name == column.name || x.alias == Some(column.name) }) {
              c.error(c.enclosingPosition, "Column " + column.fullName + " does not exist.")
            }
          }
        }
      }.getOrElse {
        c.error(c.enclosingPosition, "Table " + column.table + " does not exist.")
      }
    }

    others.foreach(_.validate(c, schema))
  }
}

class ColumnModel {
  var name: String = null
  var table: Option[String] = None
  var alias: Option[String] = None

  def fullName = {
    table.map(_ + "." + name).getOrElse(name)
  }

  override def toString(): String = {
    s"Column(table: $table, name: $name, alias: $alias)"
  }
}

