package better.jdbc

import java.sql._

import better.files._

import scala.reflect.ClassTag

class DB(conn: Connection, typeMapper: TypeMapper) extends ManagedResource[Connection] {

  def update(template: SqlTemplate): Int = {
    execute(conn, template){ stmt =>
      stmt.executeUpdate()
    }
  }

  def selectFirst[T](template: SqlTemplate)(f: ResultSet => T): Option[T] = {
    execute(conn, template){ stmt =>
      (for {
        rs <- stmt.executeQuery().autoClosed
      } yield {
        if(rs.next){
          Some(f(rs))
        } else {
          None
        }
      }).head
    }
  }

  def selectFirst[P1, T](template: SqlTemplate, f: (P1) => T)(implicit c1: ClassTag[P1]): Option[T] = {
    selectFirst(template)(rs => f(typeMapper.get[P1](rs, 1)))
  }

  def selectFirst[P1, P2, T](template: SqlTemplate, f: (P1, P2) => T)(implicit c1: ClassTag[P1], c2: ClassTag[P2]): Option[T] = {
    selectFirst(template)(rs => f(typeMapper.get[P1](rs, 1), typeMapper.get[P2](rs, 2)))
  }

  def selectFirst[P1, P2, P3, T](template: SqlTemplate, f: (P1, P2, P3) => T)(implicit c1: ClassTag[P1], c2: ClassTag[P2], c3: ClassTag[P3]): Option[T] = {
    selectFirst(template)(rs => f(typeMapper.get[P1](rs, 1), typeMapper.get[P2](rs, 2), typeMapper.get[P3](rs, 3)))
  }

  def selectFirst[P1, P2, P3, P4, T](template: SqlTemplate, f: (P1, P2, P3, P4) => T)(implicit c1: ClassTag[P1], c2: ClassTag[P2], c3: ClassTag[P3], c4: ClassTag[P4]): Option[T] = {
    selectFirst(template)(rs => f(typeMapper.get[P1](rs, 1), typeMapper.get[P2](rs, 2), typeMapper.get[P3](rs, 3), typeMapper.get[P4](rs, 4)))
  }

  def selectFirst[P1, P2, P3, P4, P5, T](template: SqlTemplate, f: (P1, P2, P3, P4, P5) => T)(implicit c1: ClassTag[P1], c2: ClassTag[P2], c3: ClassTag[P3], c4: ClassTag[P4], c5: ClassTag[P5]): Option[T] = {
    selectFirst(template)(rs => f(typeMapper.get[P1](rs, 1), typeMapper.get[P2](rs, 2), typeMapper.get[P3](rs, 3), typeMapper.get[P4](rs, 4), typeMapper.get[P5](rs, 5)))
  }

  def select[T](template: SqlTemplate)(f: ResultSet => T): Seq[T] = {
    execute(conn, template){ stmt =>
      (for {
        rs <- stmt.executeQuery().autoClosed
      } yield {
        val list = new scala.collection.mutable.ListBuffer[T]
        while(rs.next){
          list += f(rs)
        }
        list.toSeq
      }).head
    }
  }

  def select[P1, T](template: SqlTemplate, f: (P1) => T)(implicit c1: ClassTag[P1]): Seq[T] = {
    select(template)(rs => f(typeMapper.get[P1](rs, 1)))
  }

  def select[P1, P2, T](template: SqlTemplate, f: (P1, P2) => T)(implicit c1: ClassTag[P1], c2: ClassTag[P2]): Seq[T] = {
    select(template)(rs => f(typeMapper.get[P1](rs, 1), typeMapper.get[P2](rs, 2)))
  }

  def select[P1, P2, P3, T](template: SqlTemplate, f: (P1, P2, P3) => T)(implicit c1: ClassTag[P1], c2: ClassTag[P2], c3: ClassTag[P3]): Seq[T] = {
    select(template)(rs => f(typeMapper.get[P1](rs, 1), typeMapper.get[P2](rs, 2), typeMapper.get[P3](rs, 3)))
  }

  def select[P1, P2, P3, P4, T](template: SqlTemplate, f: (P1, P2, P3, P4) => T)(implicit c1: ClassTag[P1], c2: ClassTag[P2], c3: ClassTag[P3], c4: ClassTag[P4]): Seq[T] = {
    select(template)(rs => f(typeMapper.get[P1](rs, 1), typeMapper.get[P2](rs, 2), typeMapper.get[P3](rs, 3), typeMapper.get[P4](rs, 4)))
  }

  def select[P1, P2, P3, P4, P5, T](template: SqlTemplate, f: (P1, P2, P3, P4, P5) => T)(implicit c1: ClassTag[P1], c2: ClassTag[P2], c3: ClassTag[P3], c4: ClassTag[P4], c5: ClassTag[P5]): Seq[T] = {
    select(template)(rs => f(typeMapper.get[P1](rs, 1), typeMapper.get[P2](rs, 2), typeMapper.get[P3](rs, 3), typeMapper.get[P4](rs, 4), typeMapper.get[P5](rs, 5)))
  }

//  def select[P1, P2, P3, P4, P5, P6, T](template: SqlTemplate, f: (P1, P2, P3, P4, P5, P6) => T): Seq[T] = {
//    select(template)(rs => f(typeMapper.get[P1](rs, 1), typeMapper.get[P2](rs, 2), typeMapper.get[P3](rs, 3), typeMapper.get[P4](rs, 4), typeMapper.get[P5](rs, 5), typeMapper.get[P6](rs, 6)))
//  }
//
//  def select[P1, P2, P3, P4, P5, P6, P7, T](template: SqlTemplate, f: (P1, P2, P3, P4, P5, P6, P7) => T): Seq[T] = {
//    select(template)(rs => f(typeMapper.get[P1](rs, 1), typeMapper.get[P2](rs, 2), typeMapper.get[P3](rs, 3), typeMapper.get[P4](rs, 4), typeMapper.get[P5](rs, 5), typeMapper.get[P6](rs, 6), typeMapper.get[P7](rs, 7)))
//  }
//
//  def select[P1, P2, P3, P4, P5, P6, P7, P8, T](template: SqlTemplate, f: (P1, P2, P3, P4, P5, P6, P7, P8) => T): Seq[T] = {
//    select(template)(rs => f(typeMapper.get[P1](rs, 1), typeMapper.get[P2](rs, 2), typeMapper.get[P3](rs, 3), typeMapper.get[P4](rs, 4), typeMapper.get[P5](rs, 5), typeMapper.get[P6](rs, 6), typeMapper.get[P7](rs, 7), typeMapper.get[P8](rs, 8)))
//  }
//
//  def select[P1, P2, P3, P4, P5, P6, P7, P8, P9, T](template: SqlTemplate, f: (P1, P2, P3, P4, P5, P6, P7, P8, P9) => T): Seq[T] = {
//    select(template)(rs => f(typeMapper.get[P1](rs, 1), typeMapper.get[P2](rs, 2), typeMapper.get[P3](rs, 3), typeMapper.get[P4](rs, 4), typeMapper.get[P5](rs, 5), typeMapper.get[P6](rs, 6), typeMapper.get[P7](rs, 7), typeMapper.get[P8](rs, 8), typeMapper.get[P9](rs, 9)))
//  }
//
//  def select[P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, T](template: SqlTemplate, f: (P1, P2, P3, P4, P5, P6, P7, P8, P9, P10) => T): Seq[T] = {
//    select(template)(rs => f(typeMapper.get[P1](rs, 1), typeMapper.get[P2](rs, 2), typeMapper.get[P3](rs, 3), typeMapper.get[P4](rs, 4), typeMapper.get[P5](rs, 5), typeMapper.get[P6](rs, 6), typeMapper.get[P7](rs, 7), typeMapper.get[P8](rs, 8), typeMapper.get[P9](rs, 9), typeMapper.get[P10](rs, 10)))
//  }
//
//  def select[P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, T](template: SqlTemplate, f: (P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11) => T): Seq[T] = {
//    select(template)(rs => f(typeMapper.get[P1](rs, 1), typeMapper.get[P2](rs, 2), typeMapper.get[P3](rs, 3), typeMapper.get[P4](rs, 4), typeMapper.get[P5](rs, 5), typeMapper.get[P6](rs, 6), typeMapper.get[P7](rs, 7), typeMapper.get[P8](rs, 8), typeMapper.get[P9](rs, 9), typeMapper.get[P10](rs, 10), typeMapper.get[P11](rs, 11)))
//  }

  def selectInt(template: SqlTemplate): Option[Int] = {
    selectFirst(template)(_.getInt(1))
  }

  def selectString(template: SqlTemplate): Option[String] = {
    selectFirst(template)(_.getString(1))
  }

  override def foreach[U](f: (Connection) => U): Unit = try {
    f(conn)
  } finally {
    conn.close()
  }

  protected def execute[T](conn: Connection, template: SqlTemplate)(f: (PreparedStatement) => T): T = {
    (for {
      stmt <- conn.prepareStatement(template.sql).autoClosed
    } yield {
      template.params.zipWithIndex.foreach { case (x, i) =>
        typeMapper.set(stmt, i + 1, x)
      }
      f(stmt)
    }).head
  }

}

