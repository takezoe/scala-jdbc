package com.github.takezoe.scala.jdbc.validation

import java.io.{File, FileInputStream}

import com.github.takezoe.scala.jdbc.IOUtils._
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule

case class SchemaDef(tables: Seq[TableDef], connection: Option[ConnectionDef]){
  def toMap: Map[String, TableDef] = {
    tables.map { t => t.name -> t }.toMap
  }
}

case class ConnectionDef(driver: String, url: String, user: String, password: String)

case class TableDef(name:String, columns: Seq[ColumnDef])

case class ColumnDef(name: String)

object SchemaDef {

  private val mapper = new ObjectMapper()
  mapper.enable(DeserializationFeature.UNWRAP_SINGLE_VALUE_ARRAYS)
  mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
  mapper.registerModule(DefaultScalaModule)

  def load(): Option[SchemaDef] = {
    val file = new File("schema.json")
    if(file.exists){
      // Load from file system
      val json = using(new FileInputStream(file)){ in =>
        readStreamAsString(in)
      }
      Some(mapper.readValue(json, classOf[SchemaDef]))
    } else {
      val in = Thread.currentThread.getContextClassLoader.getResourceAsStream("schema.json")
      Option(in).map { in =>
        // Load from classpath
        val json = using(in){ in =>
          readStreamAsString(in)
        }
        mapper.readValue(json, classOf[SchemaDef])
      }
    }
  }
}