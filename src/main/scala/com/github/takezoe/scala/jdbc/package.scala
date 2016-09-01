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
import net.sf.jsqlparser.statement.select.{Select, _}
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
  val columns = new ListBuffer[ColumnDef]

  override def toString(): String = {
    s"Table(name: $name, alias: $alias, columns: $columns)"
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

  class TableExtractor extends SelectVisitorAdapter {

    var table = new TableDef()

    override def visit(plainSelect: PlainSelect): Unit = {
      Option(plainSelect.getJoins).map(_.asScala.foreach { join =>
        println("SubJoin: Right=" + join.getRightItem + ", " + join.getOnExpression + ", " + Option(join.getUsingColumns).map(_.asScala).orNull)
      })
      plainSelect.getFromItem.accept(new FromItemVisitorAdapter {
        override def visit(tableName: Table): Unit = {
          println("----")
          println(tableName.getName)
          println("----")
          table.name = tableName.getName
          table.alias = tableName.getAlias.getName
        }

        override def visit(subSelect: SubSelect): Unit = {
          val visitor = new TableExtractor()
          subSelect.getSelectBody.accept(visitor)
          Option(subSelect.getAlias).map(_.getName).orNull
          table.name = "<subquery>"
          table.alias = Option(subSelect.getAlias).map(_.getName).orNull
        }
      })

      plainSelect.getSelectItems.asScala.foreach { item =>
        item.accept(new SelectItemVisitorAdapter {
          override def visit(columns: AllColumns): Unit = {
            val column = new ColumnDef()
            column.name = "*"
            if(table.alias == null){
              column.table = table.name
            } else {
              column.table = table.alias
            }
            table.columns += column
          }
          override def visit(selectExpressionItem: SelectExpressionItem): Unit = {
            selectExpressionItem.getExpression.accept(new ExpressionVisitorAdapter {
              override def visit(tableColumn: Column): Unit = {
                val column = new ColumnDef()
                column.name = tableColumn.getColumnName
                column.table = tableColumn.getTable.getName
                column.alias = Option(selectExpressionItem.getAlias).map(_.getName).orNull
                table.columns += column
              }
            })
          }
        })

      }

      println(table)
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
                val visitor = new TableExtractor()
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
