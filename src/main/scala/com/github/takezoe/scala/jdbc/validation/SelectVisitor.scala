package com.github.takezoe.scala.jdbc.validation

import net.sf.jsqlparser.expression.ExpressionVisitorAdapter
import net.sf.jsqlparser.schema.{Column, Table}
import net.sf.jsqlparser.statement.select._

import scala.collection.JavaConverters._

class SelectVisitor extends SelectVisitorAdapter {

  var select = new SelectModel()

  override def visit(plainSelect: PlainSelect): Unit = {
    Option(plainSelect.getJoins).map(_.asScala.foreach { join =>
      val visitor = new FromItemVisitor()
      join.getRightItem.accept(visitor)
      select.from += visitor.table
    })

    val visitor = new FromItemVisitor()
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
              column.table = tableColumn.getTable.getName
              column.alias = Option(selectExpressionItem.getAlias).map(_.getName).orNull
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
        c.table = Option(column.getTable).map(_.getName).orNull
        select.where += c
      }
    }))
    Option(plainSelect.getOrderByElements).foreach(_.asScala.foreach { orderBy =>
      println("OrderBy: " + orderBy)
      orderBy.accept(new OrderByVisitor {
        override def visit(orderBy: OrderByElement): Unit = {
          orderBy.getExpression.accept(new ExpressionVisitorAdapter(){
            override def visit(column: Column): Unit = {
              val c = new ColumnModel()
              c.name = column.getColumnName
              c.table = Option(column.getTable).map(_.getName).orNull
              select.orderBy += c
            }
          })
        }
      })
    })

    println(select)
    select.validate()
  }

  class FromItemVisitor extends FromItemVisitorAdapter {

    val table = new TableModel()

    override def visit(tableName: Table): Unit = {
      table.name = tableName.getName
      table.alias = Option(tableName.getAlias).map(_.getName).orNull
    }

    override def visit(subSelect: SubSelect): Unit = {
      val visitor = new SelectVisitor()
      subSelect.getSelectBody.accept(visitor)
      table.select = visitor.select
      table.alias = Option(subSelect.getAlias).map(_.getName).orNull
    }
  }
}