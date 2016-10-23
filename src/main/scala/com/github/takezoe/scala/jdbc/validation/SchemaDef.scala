package com.github.takezoe.scala.jdbc.validation

import java.io.{File, FileInputStream}

import com.github.takezoe.scala.jdbc.IOUtils._
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule

case class ConnectionDef(driver: String, url: String, user: String, password: String)

object ConnectionDef {
  private val mapper = new ObjectMapper()
  mapper.enable(DeserializationFeature.UNWRAP_SINGLE_VALUE_ARRAYS)
  mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
  mapper.registerModule(DefaultScalaModule)

  // TODO Load using typesafe-config, rather than JSON?
  def load(): Option[ConnectionDef] = {
    val file = new File("database.json")
    if(file.exists){
      // Load from file system
      val json = using(new FileInputStream(file)){ in =>
        readStreamAsString(in)
      }
      Some(mapper.readValue(json, classOf[ConnectionDef]))
    } else {
      val in = Thread.currentThread.getContextClassLoader.getResourceAsStream("database.json")
      Option(in).map { in =>
        // Load from classpath
        val json = using(in){ in =>
          readStreamAsString(in)
        }
        mapper.readValue(json, classOf[ConnectionDef])
      }
    }
  }
}