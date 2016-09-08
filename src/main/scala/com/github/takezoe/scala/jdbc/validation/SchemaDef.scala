package com.github.takezoe.scala.jdbc.validation

case class SchemaDef(tables: Seq[TableDef])

case class TableDef(name:String, columns: Seq[ColumnDef])

case class ColumnDef(name: String)
