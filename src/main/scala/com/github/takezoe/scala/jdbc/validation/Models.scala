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

  override def toString(): String = {
    s"Select(from: $from, columns: $columns, orderBy; $orderBy, where: $where)"
  }

  def validate(c: Context): Unit = {
    from.foreach { table =>
      table.select.right.foreach(_.validate(c))
    }
    (columns ++ where ++ orderBy).foreach { column =>
      val table = column.table.map { t =>
        from.find(x => x.select == Left(t) || x.alias == Some(t))
      }.getOrElse {
        from.headOption
      }

      table.map { table =>
        table.select match {
          case Left(name) => {
            // TODO Check with table definition which would be given from user
          }
          case Right(select) => {
            if (!select.columns.exists(x => x.name == column.name || x.alias == Some(column.name))) {
              c.error(c.enclosingPosition, "Column " + column.fullName + " does not exist.")
            }
          }
        }
      }.getOrElse {
        c.error(c.enclosingPosition, "Table " + column.table + " does not exist.")
      }
    }
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

