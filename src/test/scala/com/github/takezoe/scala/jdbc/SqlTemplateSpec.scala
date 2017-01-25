package com.github.takezoe.scala.jdbc

import java.sql.DriverManager

import org.scalatest.FunSuite

class SqlTemplateSpec extends FunSuite {

  test("set parameters"){
    val a = Article(1, 1, "takezoe", "Database access in Scala", "...")
    DB.autoClose(DriverManager.getConnection("jdbc:h2:mem:test;TRACE_LEVEL_FILE=4")) { db =>
      db.update(sql"""CREATE TABLE articles (
        id serial PRIMARY KEY,
        published bigint NOT NULL,
        author varchar (50) NOT NULL,
        title varchar (100) NOT NULL,
        text varchar (5000) NOT NULL
      )""")

      db.update(sql"INSERT INTO articles (published,author,title,text) VALUES (${a.ms},${a.author},${a.title},${a.text})")
    }
  }

}

case class Article(val id: Long, val ms: Long, val author: String, val title: String, val text: String)