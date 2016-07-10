package com.github.takezoe.scala.jdbc

import java.sql._
import scala.reflect.ClassTag

class TypeMapper {

  def set(stmt: PreparedStatement, index: Int, value: Any): Unit = {
    value match {
      case x: Int       => stmt.setInt(index, x)
      case x: Long      => stmt.setLong(index, x)
      case x: Double    => stmt.setDouble(index, x)
      case x: Short     => stmt.setShort(index, x)
      case x: Float     => stmt.setFloat(index, x)
      case x: Timestamp => stmt.setTimestamp(index, x)
      case x: Date      => stmt.setDate(index, x)
      case x: Time      => stmt.setTime(index, x)
      case x: String    => stmt.setString(index, x)
      case _ => throw new UnsupportedOperationException(s"Unsupported type: ${value.getClass.getName}")
    }
  }

  def get[T](rs: ResultSet, index: Int)(implicit m: ClassTag[T]): T = {
    val c = m.runtimeClass
    (if(c.isAssignableFrom(classOf[Int])) {
      rs.getInt(index)
    } else if(c.isAssignableFrom(classOf[Long])) {
      rs.getLong(index)
    } else if(c.isAssignableFrom(classOf[Double])) {
      rs.getDouble(index)
    } else if(c.isAssignableFrom(classOf[Float])) {
      rs.getFloat(index)
    } else if(c.isAssignableFrom(classOf[Short])) {
      rs.getShort(index)
    } else if(c.isAssignableFrom(classOf[Timestamp])) {
      rs.getTimestamp(index)
    } else if(c.isAssignableFrom(classOf[Date])) {
      rs.getDate(index)
    } else if(c.isAssignableFrom(classOf[Time])) {
      rs.getTime(index)
    } else if(c.isAssignableFrom(classOf[String])){
      rs.getString(index)
    } else {
      throw new UnsupportedOperationException(s"Unsupported type: ${c.getName}")
    }).asInstanceOf[T]
  }

}