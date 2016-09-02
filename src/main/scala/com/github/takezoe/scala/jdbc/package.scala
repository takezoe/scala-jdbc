package com.github.takezoe.scala

import net.sf.jsqlparser.JSQLParserException
import net.sf.jsqlparser.expression.operators.arithmetic.{BitwiseAnd, BitwiseOr, Concat, Multiplication, _}
import net.sf.jsqlparser.expression.operators.conditional.{AndExpression, OrExpression}
import net.sf.jsqlparser.expression.{CastExpression, Function, IntervalExpression, KeepExpression, _}
import net.sf.jsqlparser.expression.operators.relational.{EqualsTo, LikeExpression, MinorThan, MinorThanEquals, _}
import net.sf.jsqlparser.parser.CCJSqlParserUtil
import net.sf.jsqlparser.schema.{Column, Table}
import net.sf.jsqlparser.statement.alter.Alter
import net.sf.jsqlparser.statement.create.index.CreateIndex
import net.sf.jsqlparser.statement.create.table.CreateTable
import net.sf.jsqlparser.statement.create.view.{AlterView, CreateView}
import net.sf.jsqlparser.statement.delete.Delete
import net.sf.jsqlparser.statement.drop.Drop
import net.sf.jsqlparser.statement.{SetStatement, StatementVisitor, StatementVisitorAdapter, Statements}
import net.sf.jsqlparser.statement.execute.Execute
import net.sf.jsqlparser.statement.insert.Insert
import net.sf.jsqlparser.statement.merge.Merge
import net.sf.jsqlparser.statement.replace.Replace
import net.sf.jsqlparser.statement.select.{FromItemVisitorAdapter, Select, _}
import net.sf.jsqlparser.statement.truncate.Truncate
import net.sf.jsqlparser.statement.update.Update

import scala.language.experimental.macros
import scala.reflect.macros.blackbox.Context
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

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
      val sql = sc.parts.mkString
      println(sql)
      val parse = CCJSqlParserUtil.parse(sql)
      SqlTemplate(sql, args.toSeq)
    }
  }

  case class SqlTemplate(sql: String, params: Any*)

  def sqlc(sql: String): String = macro Macros.sqlMacro

}

class TableDef {
  var name: String = null
  var alias: String = null
  var select: SelectDef = null

  override def toString(): String = {
    s"Select(name: $name, alias: $alias, select: $select)"
  }
}

class SelectDef {
  var from = new ListBuffer[TableDef]
  val columns = new ListBuffer[ColumnDef]

  override def toString(): String = {
    s"Select(from: $from, columns: $columns)"
  }

  def validate() = {
    columns.foreach { column =>
      val table = if (column.table != null){
        from.find(x => x.name == column.table || x.alias == column.table)
      } else {
        from.headOption
      }
      table.map { table =>
        if(table.name != null){

        } else {
          if (!table.select.columns.exists(x => x.name == column.name || x.alias == column.name)) {
            println("[ERROR]Column " + column + " does not exist!")
          }
        }
      }.getOrElse {
        println("[ERROR]Table " + column.table + " does not exist!")
      }
    }
  }
}

class ColumnDef {
  var name: String = null
  var table: String = null
  var alias: String = null

  override def toString(): String = {
    s"Column(table: $table, name: $name, alias: $alias)"
  }
}

object Macros {

  class FromExtractor extends FromItemVisitorAdapter {

    val table = new TableDef()

    override def visit(tableName: Table): Unit = {
      table.name = tableName.getName
      table.alias = Option(tableName.getAlias).map(_.getName).orNull
    }

    override def visit(subSelect: SubSelect): Unit = {
      val visitor = new SelectDefExtractor()
      subSelect.getSelectBody.accept(visitor)
      table.select = visitor.select
      table.alias = Option(subSelect.getAlias).map(_.getName).orNull
    }
  }

  class SelectDefExtractor extends SelectVisitorAdapter {

    var select = new SelectDef()

    override def visit(plainSelect: PlainSelect): Unit = {
      Option(plainSelect.getJoins).map(_.asScala.foreach { join =>
        val visitor = new FromExtractor()
        join.getRightItem.accept(visitor)
        select.from += visitor.table
      })

      val visitor = new FromExtractor()
      plainSelect.getFromItem.accept(visitor)
      select.from += visitor.table

      plainSelect.getSelectItems.asScala.foreach { item =>
        item.accept(new SelectItemVisitorAdapter {
          override def visit(columns: AllColumns): Unit = {
            val column = new ColumnDef()
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
                val column = new ColumnDef()
                column.name = tableColumn.getColumnName
                column.table = tableColumn.getTable.getName
                column.alias = Option(selectExpressionItem.getAlias).map(_.getName).orNull
                select.columns += column
              }
            })
          }
        })

      }

      println(select)
      select.validate()
    }
  }

  def sqlMacro(c: Context)(sql: c.Expr[String]): c.Expr[String] = {
    import c.universe._
    sql.tree match {
      case Literal(x) => x.value match {
        case s: String => {
          try {
            val parse = CCJSqlParserUtil.parse(s)
            parse.accept(new StatementVisitorAdapter {
              override def visit(select: net.sf.jsqlparser.statement.select.Select): Unit = {
                val visitor = new SelectDefExtractor()
                select.getSelectBody.accept(visitor)
              }
            })
          } catch {
            case e: JSQLParserException =>
              c.error(sql.tree.pos, e.getCause.getMessage)
s          }
        }
      }
    }
    c.Expr[String](q"$sql")
  }

}
