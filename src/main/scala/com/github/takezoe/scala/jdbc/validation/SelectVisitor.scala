package com.github.takezoe.scala.jdbc.validation

import net.sf.jsqlparser.expression.ExpressionVisitorAdapter
import net.sf.jsqlparser.schema.{Column, Table}
import net.sf.jsqlparser.statement.select._

import scala.reflect.macros.blackbox.Context
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

class SelectVisitor(c: Context) extends SelectVisitorAdapter {

  var select = new SelectModel()

  override def visit(plainSelect: PlainSelect): Unit = {
    Option(plainSelect.getJoins).map(_.asScala.foreach { join =>
      val visitor = new FromItemVisitor(c)
      join.getRightItem.accept(visitor)
      select.from += visitor.table

      Option(join.getOnExpression).foreach(_.accept(new ExpressionVisitorAdapter(){
        override def visit(tableColumn: Column): Unit = {
          val column = new ColumnModel()
          column.name = tableColumn.getColumnName
          column.table = Option(tableColumn.getTable.getName)
          column.alias = None
          select.columns += column
        }
      }))
    })

    val visitor = new FromItemVisitor(c)
    plainSelect.getFromItem.accept(visitor)
    select.from += visitor.table

    plainSelect.getSelectItems.asScala.foreach { item =>
      item.accept(new SelectItemVisitorAdapter {
        override def visit(columns: AllColumns): Unit = {
          val column = new ColumnModel()
          column.name = "*"
          //            if(table.alias == null){
          //              column.table = table.name
          //            } else {
          //              column.table = table.alias
          //            }
          select.columns += column
        }
        override def visit(selectExpressionItem: SelectExpressionItem): Unit = {
          selectExpressionItem.getExpression.accept(new ExpressionVisitorAdapter {
            override def visit(tableColumn: Column): Unit = {
              val column = new ColumnModel()
              column.name = tableColumn.getColumnName
              column.table = Option(tableColumn.getTable.getName)
              column.alias = Option(selectExpressionItem.getAlias).map(_.getName)
              select.columns += column
            }
          })
        }
      })

    }

    Option(plainSelect.getWhere).foreach(_.accept(new ExpressionVisitorAdapter {
      override def visit(column: Column): Unit = {
        val c = new ColumnModel()
        c.name = column.getColumnName
        c.table = Option(column.getTable).flatMap(t => Option(t.getName))
        select.where += c
      }

      override def visit(subSelect: SubSelect): Unit = {
        val visitor = new SelectVisitor(c)
        subSelect.getSelectBody.accept(visitor)
        select.others += visitor.select

      }
    }))
    Option(plainSelect.getOrderByElements).foreach(_.asScala.foreach { orderBy =>
      orderBy.accept(new OrderByVisitor {
        override def visit(orderBy: OrderByElement): Unit = {
          orderBy.getExpression.accept(new ExpressionVisitorAdapter(){
            override def visit(column: Column): Unit = {
              val c = new ColumnModel()
              c.name = column.getColumnName
              c.table = Option(column.getTable).map(_.getName)
              select.orderBy += c
            }
          })
        }
      })
    })
  }

  class FromItemVisitor(c: Context) extends FromItemVisitorAdapter {

    val table = new TableModel()

    override def visit(tableName: Table): Unit = {
      table.select = Left(tableName.getName)
      table.alias = Option(tableName.getAlias).map(_.getName)
    }

    override def visit(subSelect: SubSelect): Unit = {
      val visitor = new SelectVisitor(c)
      subSelect.getSelectBody.accept(visitor)
      table.select = Right(visitor.select)
      table.alias = Option(subSelect.getAlias).map(_.getName)
    }
  }
}

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
    s"Select(from: $from, columns: $columns, orderBy; $orderBy, where: $where, others: $others)"
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
              case None => if(schema.nonEmpty){
                c.error(c.enclosingPosition, "Table " + name + " does not exist.")
              }
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
