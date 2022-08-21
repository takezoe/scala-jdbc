package com.github.takezoe.scala.jdbc

import java.sql.DriverManager

import org.scalatest.FunSuite

class SqlTemplateSpec extends FunSuite {

  test("set parameters"){
    val a = Article(0, System.currentTimeMillis, "takezoe", "Database access in Scala")
    Class.forName("org.h2.Driver")
    DB.autoClose(DriverManager.getConnection("jdbc:h2:mem:test;TRACE_LEVEL_FILE=4")) { db =>
      db.update(sql"""CREATE TABLE articles (
        id serial PRIMARY KEY,
        published bigint NOT NULL,
        author varchar (50) NOT NULL,
        title varchar (100) NOT NULL
      )""")

      db.update(sql"INSERT INTO articles (published,author,title) VALUES (${a.published},${a.author},${a.title})")

      val aricles = db.select("SELECT * FROM articles", Article.apply _)
      assert(aricles.size == 1)
    }
  }

}

case class Article(id: Long, published: Long, author: String, title: String)