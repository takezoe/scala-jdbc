package better.jdbc

import java.sql._

import better.files._

import scala.reflect.ClassTag

class DB(conn: Connection, typeMapper: TypeMapper){

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

  def selectFirst[P1, T](template: SqlTemplate, f: (P1) => T): Option[T] =
    selectFirst(template)(rs => f(typeMapper.get[P1](rs, 1)))

  def selectFirst[P1, P2, T](template: SqlTemplate, f: (P1, P2) => T): Option[T] =
    selectFirst(template)(rs => f(typeMapper.get[P1](rs, 1), typeMapper.get[P2](rs, 2)))

  def selectFirst[P1, P2, P3, T](template: SqlTemplate, f: (P1, P2, P3) => T): Option[T] =
    selectFirst(template)(rs => f(typeMapper.get[P1](rs, 1), typeMapper.get[P2](rs, 2), typeMapper.get[P3](rs, 3)))

  def selectFirst[P1, P2, P3, P4, T](template: SqlTemplate, f: (P1, P2, P3, P4) => T): Option[T] =
    selectFirst(template)(rs => f(typeMapper.get[P1](rs, 1), typeMapper.get[P2](rs, 2), typeMapper.get[P3](rs, 3), typeMapper.get[P4](rs, 4)))

  def selectFirst[P1, P2, P3, P4, P5, T](template: SqlTemplate, f: (P1, P2, P3, P4, P5) => T): Option[T] =
    selectFirst(template)(rs => f(typeMapper.get[P1](rs, 1), typeMapper.get[P2](rs, 2), typeMapper.get[P3](rs, 3), typeMapper.get[P4](rs, 4), typeMapper.get[P5](rs, 5)))

  def selectFirst[P1, P2, P3, P4, P5, P6, T](template: SqlTemplate, f: (P1, P2, P3, P4, P5, P6) => T): Option[T] =
    selectFirst(template)(rs => f(typeMapper.get[P1](rs, 1), typeMapper.get[P2](rs, 2), typeMapper.get[P3](rs, 3), typeMapper.get[P4](rs, 4), typeMapper.get[P5](rs, 5), typeMapper.get[P6](rs, 6)))

  def selectFirst[P1, P2, P3, P4, P5, P6, P7, T](template: SqlTemplate, f: (P1, P2, P3, P4, P5, P6, P7) => T): Option[T] =
    selectFirst(template)(rs => f(typeMapper.get[P1](rs, 1), typeMapper.get[P2](rs, 2), typeMapper.get[P3](rs, 3), typeMapper.get[P4](rs, 4), typeMapper.get[P5](rs, 5), typeMapper.get[P6](rs, 6), typeMapper.get[P7](rs, 7)))

  def selectFirst[P1, P2, P3, P4, P5, P6, P7, P8, T](template: SqlTemplate, f: (P1, P2, P3, P4, P5, P6, P7, P8) => T): Option[T] =
    selectFirst(template)(rs => f(typeMapper.get[P1](rs, 1), typeMapper.get[P2](rs, 2), typeMapper.get[P3](rs, 3), typeMapper.get[P4](rs, 4), typeMapper.get[P5](rs, 5), typeMapper.get[P6](rs, 6), typeMapper.get[P7](rs, 7), typeMapper.get[P8](rs, 8)))

  def selectFirst[P1, P2, P3, P4, P5, P6, P7, P8, P9, T](template: SqlTemplate, f: (P1, P2, P3, P4, P5, P6, P7, P8, P9) => T): Option[T] =
    selectFirst(template)(rs => f(typeMapper.get[P1](rs, 1), typeMapper.get[P2](rs, 2), typeMapper.get[P3](rs, 3), typeMapper.get[P4](rs, 4), typeMapper.get[P5](rs, 5), typeMapper.get[P6](rs, 6), typeMapper.get[P7](rs, 7), typeMapper.get[P8](rs, 8), typeMapper.get[P9](rs, 9)))

  def selectFirst[P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, T](template: SqlTemplate, f: (P1, P2, P3, P4, P5, P6, P7, P8, P9, P10) => T): Option[T] =
    selectFirst(template)(rs => f(typeMapper.get[P1](rs, 1), typeMapper.get[P2](rs, 2), typeMapper.get[P3](rs, 3), typeMapper.get[P4](rs, 4), typeMapper.get[P5](rs, 5), typeMapper.get[P6](rs, 6), typeMapper.get[P7](rs, 7), typeMapper.get[P8](rs, 8), typeMapper.get[P9](rs, 9), typeMapper.get[P10](rs, 10)))

  def selectFirst[P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, T](template: SqlTemplate, f: (P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11) => T): Option[T] =
    selectFirst(template)(rs => f(typeMapper.get[P1](rs, 1), typeMapper.get[P2](rs, 2), typeMapper.get[P3](rs, 3), typeMapper.get[P4](rs, 4), typeMapper.get[P5](rs, 5), typeMapper.get[P6](rs, 6), typeMapper.get[P7](rs, 7), typeMapper.get[P8](rs, 8), typeMapper.get[P9](rs, 9), typeMapper.get[P10](rs, 10), typeMapper.get[P11](rs, 11)))

  def selectFirst[P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, T](template: SqlTemplate, f: (P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12) => T): Option[T] =
    selectFirst(template)(rs => f(typeMapper.get[P1](rs, 1), typeMapper.get[P2](rs, 2), typeMapper.get[P3](rs, 3), typeMapper.get[P4](rs, 4), typeMapper.get[P5](rs, 5), typeMapper.get[P6](rs, 6), typeMapper.get[P7](rs, 7), typeMapper.get[P8](rs, 8), typeMapper.get[P9](rs, 9), typeMapper.get[P10](rs, 10), typeMapper.get[P11](rs, 11), typeMapper.get[P12](rs, 12)))

  def selectFirst[P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, T](template: SqlTemplate, f: (P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13) => T): Option[T] =
    selectFirst(template)(rs => f(typeMapper.get[P1](rs, 1), typeMapper.get[P2](rs, 2), typeMapper.get[P3](rs, 3), typeMapper.get[P4](rs, 4), typeMapper.get[P5](rs, 5), typeMapper.get[P6](rs, 6), typeMapper.get[P7](rs, 7), typeMapper.get[P8](rs, 8), typeMapper.get[P9](rs, 9), typeMapper.get[P10](rs, 10), typeMapper.get[P11](rs, 11), typeMapper.get[P12](rs, 12), typeMapper.get[P13](rs, 13)))

  def selectFirst[P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, T](template: SqlTemplate, f: (P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14) => T): Option[T] =
    selectFirst(template)(rs => f(typeMapper.get[P1](rs, 1), typeMapper.get[P2](rs, 2), typeMapper.get[P3](rs, 3), typeMapper.get[P4](rs, 4), typeMapper.get[P5](rs, 5), typeMapper.get[P6](rs, 6), typeMapper.get[P7](rs, 7), typeMapper.get[P8](rs, 8), typeMapper.get[P9](rs, 9), typeMapper.get[P10](rs, 10), typeMapper.get[P11](rs, 11), typeMapper.get[P12](rs, 12), typeMapper.get[P13](rs, 13), typeMapper.get[P14](rs, 14)))

  def selectFirst[P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, T](template: SqlTemplate, f: (P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15) => T): Option[T] =
    selectFirst(template)(rs => f(typeMapper.get[P1](rs, 1), typeMapper.get[P2](rs, 2), typeMapper.get[P3](rs, 3), typeMapper.get[P4](rs, 4), typeMapper.get[P5](rs, 5), typeMapper.get[P6](rs, 6), typeMapper.get[P7](rs, 7), typeMapper.get[P8](rs, 8), typeMapper.get[P9](rs, 9), typeMapper.get[P10](rs, 10), typeMapper.get[P11](rs, 11), typeMapper.get[P12](rs, 12), typeMapper.get[P13](rs, 13), typeMapper.get[P14](rs, 14), typeMapper.get[P15](rs, 15)))

  def selectFirst[P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16, T](template: SqlTemplate, f: (P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16) => T): Option[T] =
    selectFirst(template)(rs => f(typeMapper.get[P1](rs, 1), typeMapper.get[P2](rs, 2), typeMapper.get[P3](rs, 3), typeMapper.get[P4](rs, 4), typeMapper.get[P5](rs, 5), typeMapper.get[P6](rs, 6), typeMapper.get[P7](rs, 7), typeMapper.get[P8](rs, 8), typeMapper.get[P9](rs, 9), typeMapper.get[P10](rs, 10), typeMapper.get[P11](rs, 11), typeMapper.get[P12](rs, 12), typeMapper.get[P13](rs, 13), typeMapper.get[P14](rs, 14), typeMapper.get[P15](rs, 15), typeMapper.get[P16](rs, 16)))

  def selectFirst[P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16, P17, T](template: SqlTemplate, f: (P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16, P17) => T): Option[T] =
    selectFirst(template)(rs => f(typeMapper.get[P1](rs, 1), typeMapper.get[P2](rs, 2), typeMapper.get[P3](rs, 3), typeMapper.get[P4](rs, 4), typeMapper.get[P5](rs, 5), typeMapper.get[P6](rs, 6), typeMapper.get[P7](rs, 7), typeMapper.get[P8](rs, 8), typeMapper.get[P9](rs, 9), typeMapper.get[P10](rs, 10), typeMapper.get[P11](rs, 11), typeMapper.get[P12](rs, 12), typeMapper.get[P13](rs, 13), typeMapper.get[P14](rs, 14), typeMapper.get[P15](rs, 15), typeMapper.get[P16](rs, 16), typeMapper.get[P17](rs, 17)))

  def selectFirst[P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16, P17, P18, T](template: SqlTemplate, f: (P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16, P17, P18) => T): Option[T] =
    selectFirst(template)(rs => f(typeMapper.get[P1](rs, 1), typeMapper.get[P2](rs, 2), typeMapper.get[P3](rs, 3), typeMapper.get[P4](rs, 4), typeMapper.get[P5](rs, 5), typeMapper.get[P6](rs, 6), typeMapper.get[P7](rs, 7), typeMapper.get[P8](rs, 8), typeMapper.get[P9](rs, 9), typeMapper.get[P10](rs, 10), typeMapper.get[P11](rs, 11), typeMapper.get[P12](rs, 12), typeMapper.get[P13](rs, 13), typeMapper.get[P14](rs, 14), typeMapper.get[P15](rs, 15), typeMapper.get[P16](rs, 16), typeMapper.get[P17](rs, 17), typeMapper.get[P18](rs, 18)))

  def selectFirst[P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16, P17, P18, P19, T](template: SqlTemplate, f: (P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16, P17, P18, P19) => T): Option[T] =
    selectFirst(template)(rs => f(typeMapper.get[P1](rs, 1), typeMapper.get[P2](rs, 2), typeMapper.get[P3](rs, 3), typeMapper.get[P4](rs, 4), typeMapper.get[P5](rs, 5), typeMapper.get[P6](rs, 6), typeMapper.get[P7](rs, 7), typeMapper.get[P8](rs, 8), typeMapper.get[P9](rs, 9), typeMapper.get[P10](rs, 10), typeMapper.get[P11](rs, 11), typeMapper.get[P12](rs, 12), typeMapper.get[P13](rs, 13), typeMapper.get[P14](rs, 14), typeMapper.get[P15](rs, 15), typeMapper.get[P16](rs, 16), typeMapper.get[P17](rs, 17), typeMapper.get[P18](rs, 18), typeMapper.get[P19](rs, 19)))

  def selectFirst[P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16, P17, P18, P19, P20, T](template: SqlTemplate, f: (P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16, P17, P18, P19, P20) => T): Option[T] =
    selectFirst(template)(rs => f(typeMapper.get[P1](rs, 1), typeMapper.get[P2](rs, 2), typeMapper.get[P3](rs, 3), typeMapper.get[P4](rs, 4), typeMapper.get[P5](rs, 5), typeMapper.get[P6](rs, 6), typeMapper.get[P7](rs, 7), typeMapper.get[P8](rs, 8), typeMapper.get[P9](rs, 9), typeMapper.get[P10](rs, 10), typeMapper.get[P11](rs, 11), typeMapper.get[P12](rs, 12), typeMapper.get[P13](rs, 13), typeMapper.get[P14](rs, 14), typeMapper.get[P15](rs, 15), typeMapper.get[P16](rs, 16), typeMapper.get[P17](rs, 17), typeMapper.get[P18](rs, 18), typeMapper.get[P19](rs, 19), typeMapper.get[P20](rs, 20)))

  def selectFirst[P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16, P17, P18, P19, P20, P21, T](template: SqlTemplate, f: (P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16, P17, P18, P19, P20, P21) => T): Option[T] =
    selectFirst(template)(rs => f(typeMapper.get[P1](rs, 1), typeMapper.get[P2](rs, 2), typeMapper.get[P3](rs, 3), typeMapper.get[P4](rs, 4), typeMapper.get[P5](rs, 5), typeMapper.get[P6](rs, 6), typeMapper.get[P7](rs, 7), typeMapper.get[P8](rs, 8), typeMapper.get[P9](rs, 9), typeMapper.get[P10](rs, 10), typeMapper.get[P11](rs, 11), typeMapper.get[P12](rs, 12), typeMapper.get[P13](rs, 13), typeMapper.get[P14](rs, 14), typeMapper.get[P15](rs, 15), typeMapper.get[P16](rs, 16), typeMapper.get[P17](rs, 17), typeMapper.get[P18](rs, 18), typeMapper.get[P19](rs, 19), typeMapper.get[P20](rs, 20), typeMapper.get[P21](rs, 21)))

  def selectFirst[P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16, P17, P18, P19, P20, P21, P22, T](template: SqlTemplate, f: (P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16, P17, P18, P19, P20, P21, P22) => T): Option[T] =
    selectFirst(template)(rs => f(typeMapper.get[P1](rs, 1), typeMapper.get[P2](rs, 2), typeMapper.get[P3](rs, 3), typeMapper.get[P4](rs, 4), typeMapper.get[P5](rs, 5), typeMapper.get[P6](rs, 6), typeMapper.get[P7](rs, 7), typeMapper.get[P8](rs, 8), typeMapper.get[P9](rs, 9), typeMapper.get[P10](rs, 10), typeMapper.get[P11](rs, 11), typeMapper.get[P12](rs, 12), typeMapper.get[P13](rs, 13), typeMapper.get[P14](rs, 14), typeMapper.get[P15](rs, 15), typeMapper.get[P16](rs, 16), typeMapper.get[P17](rs, 17), typeMapper.get[P18](rs, 18), typeMapper.get[P19](rs, 19), typeMapper.get[P20](rs, 20), typeMapper.get[P21](rs, 21), typeMapper.get[P22](rs, 22)))

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

  def select[P1, T](template: SqlTemplate, f: (P1) => T): Seq[T] =
    select(template)(rs => f(typeMapper.get[P1](rs, 1)))

  def select[P1, P2, T](template: SqlTemplate, f: (P1, P2) => T): Seq[T] =
    select(template)(rs => f(typeMapper.get[P1](rs, 1), typeMapper.get[P2](rs, 2)))

  def select[P1, P2, P3, T](template: SqlTemplate, f: (P1, P2, P3) => T): Seq[T] =
    select(template)(rs => f(typeMapper.get[P1](rs, 1), typeMapper.get[P2](rs, 2), typeMapper.get[P3](rs, 3)))

  def select[P1, P2, P3, P4, T](template: SqlTemplate, f: (P1, P2, P3, P4) => T): Seq[T] =
    select(template)(rs => f(typeMapper.get[P1](rs, 1), typeMapper.get[P2](rs, 2), typeMapper.get[P3](rs, 3), typeMapper.get[P4](rs, 4)))

  def select[P1, P2, P3, P4, P5, T](template: SqlTemplate, f: (P1, P2, P3, P4, P5) => T): Seq[T] =
    select(template)(rs => f(typeMapper.get[P1](rs, 1), typeMapper.get[P2](rs, 2), typeMapper.get[P3](rs, 3), typeMapper.get[P4](rs, 4), typeMapper.get[P5](rs, 5)))

  def select[P1, P2, P3, P4, P5, P6, T](template: SqlTemplate, f: (P1, P2, P3, P4, P5, P6) => T): Seq[T] =
    select(template)(rs => f(typeMapper.get[P1](rs, 1), typeMapper.get[P2](rs, 2), typeMapper.get[P3](rs, 3), typeMapper.get[P4](rs, 4), typeMapper.get[P5](rs, 5), typeMapper.get[P6](rs, 6)))

  def select[P1, P2, P3, P4, P5, P6, P7, T](template: SqlTemplate, f: (P1, P2, P3, P4, P5, P6, P7) => T): Seq[T] =
    select(template)(rs => f(typeMapper.get[P1](rs, 1), typeMapper.get[P2](rs, 2), typeMapper.get[P3](rs, 3), typeMapper.get[P4](rs, 4), typeMapper.get[P5](rs, 5), typeMapper.get[P6](rs, 6), typeMapper.get[P7](rs, 7)))

  def select[P1, P2, P3, P4, P5, P6, P7, P8, T](template: SqlTemplate, f: (P1, P2, P3, P4, P5, P6, P7, P8) => T): Seq[T] =
    select(template)(rs => f(typeMapper.get[P1](rs, 1), typeMapper.get[P2](rs, 2), typeMapper.get[P3](rs, 3), typeMapper.get[P4](rs, 4), typeMapper.get[P5](rs, 5), typeMapper.get[P6](rs, 6), typeMapper.get[P7](rs, 7), typeMapper.get[P8](rs, 8)))

  def select[P1, P2, P3, P4, P5, P6, P7, P8, P9, T](template: SqlTemplate, f: (P1, P2, P3, P4, P5, P6, P7, P8, P9) => T): Seq[T] =
    select(template)(rs => f(typeMapper.get[P1](rs, 1), typeMapper.get[P2](rs, 2), typeMapper.get[P3](rs, 3), typeMapper.get[P4](rs, 4), typeMapper.get[P5](rs, 5), typeMapper.get[P6](rs, 6), typeMapper.get[P7](rs, 7), typeMapper.get[P8](rs, 8), typeMapper.get[P9](rs, 9)))

  def select[P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, T](template: SqlTemplate, f: (P1, P2, P3, P4, P5, P6, P7, P8, P9, P10) => T): Seq[T] =
    select(template)(rs => f(typeMapper.get[P1](rs, 1), typeMapper.get[P2](rs, 2), typeMapper.get[P3](rs, 3), typeMapper.get[P4](rs, 4), typeMapper.get[P5](rs, 5), typeMapper.get[P6](rs, 6), typeMapper.get[P7](rs, 7), typeMapper.get[P8](rs, 8), typeMapper.get[P9](rs, 9), typeMapper.get[P10](rs, 10)))

  def select[P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, T](template: SqlTemplate, f: (P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11) => T): Seq[T] =
    select(template)(rs => f(typeMapper.get[P1](rs, 1), typeMapper.get[P2](rs, 2), typeMapper.get[P3](rs, 3), typeMapper.get[P4](rs, 4), typeMapper.get[P5](rs, 5), typeMapper.get[P6](rs, 6), typeMapper.get[P7](rs, 7), typeMapper.get[P8](rs, 8), typeMapper.get[P9](rs, 9), typeMapper.get[P10](rs, 10), typeMapper.get[P11](rs, 11)))

  def select[P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, T](template: SqlTemplate, f: (P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12) => T): Seq[T] =
    select(template)(rs => f(typeMapper.get[P1](rs, 1), typeMapper.get[P2](rs, 2), typeMapper.get[P3](rs, 3), typeMapper.get[P4](rs, 4), typeMapper.get[P5](rs, 5), typeMapper.get[P6](rs, 6), typeMapper.get[P7](rs, 7), typeMapper.get[P8](rs, 8), typeMapper.get[P9](rs, 9), typeMapper.get[P10](rs, 10), typeMapper.get[P11](rs, 11), typeMapper.get[P12](rs, 12)))

  def select[P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, T](template: SqlTemplate, f: (P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13) => T): Seq[T] =
    select(template)(rs => f(typeMapper.get[P1](rs, 1), typeMapper.get[P2](rs, 2), typeMapper.get[P3](rs, 3), typeMapper.get[P4](rs, 4), typeMapper.get[P5](rs, 5), typeMapper.get[P6](rs, 6), typeMapper.get[P7](rs, 7), typeMapper.get[P8](rs, 8), typeMapper.get[P9](rs, 9), typeMapper.get[P10](rs, 10), typeMapper.get[P11](rs, 11), typeMapper.get[P12](rs, 12), typeMapper.get[P13](rs, 13)))

  def select[P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, T](template: SqlTemplate, f: (P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14) => T): Seq[T] =
    select(template)(rs => f(typeMapper.get[P1](rs, 1), typeMapper.get[P2](rs, 2), typeMapper.get[P3](rs, 3), typeMapper.get[P4](rs, 4), typeMapper.get[P5](rs, 5), typeMapper.get[P6](rs, 6), typeMapper.get[P7](rs, 7), typeMapper.get[P8](rs, 8), typeMapper.get[P9](rs, 9), typeMapper.get[P10](rs, 10), typeMapper.get[P11](rs, 11), typeMapper.get[P12](rs, 12), typeMapper.get[P13](rs, 13), typeMapper.get[P14](rs, 14)))

  def select[P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, T](template: SqlTemplate, f: (P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15) => T): Seq[T] =
    select(template)(rs => f(typeMapper.get[P1](rs, 1), typeMapper.get[P2](rs, 2), typeMapper.get[P3](rs, 3), typeMapper.get[P4](rs, 4), typeMapper.get[P5](rs, 5), typeMapper.get[P6](rs, 6), typeMapper.get[P7](rs, 7), typeMapper.get[P8](rs, 8), typeMapper.get[P9](rs, 9), typeMapper.get[P10](rs, 10), typeMapper.get[P11](rs, 11), typeMapper.get[P12](rs, 12), typeMapper.get[P13](rs, 13), typeMapper.get[P14](rs, 14), typeMapper.get[P15](rs, 15)))

  def select[P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16, T](template: SqlTemplate, f: (P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16) => T): Seq[T] =
    select(template)(rs => f(typeMapper.get[P1](rs, 1), typeMapper.get[P2](rs, 2), typeMapper.get[P3](rs, 3), typeMapper.get[P4](rs, 4), typeMapper.get[P5](rs, 5), typeMapper.get[P6](rs, 6), typeMapper.get[P7](rs, 7), typeMapper.get[P8](rs, 8), typeMapper.get[P9](rs, 9), typeMapper.get[P10](rs, 10), typeMapper.get[P11](rs, 11), typeMapper.get[P12](rs, 12), typeMapper.get[P13](rs, 13), typeMapper.get[P14](rs, 14), typeMapper.get[P15](rs, 15), typeMapper.get[P16](rs, 16)))

  def select[P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16, P17, T](template: SqlTemplate, f: (P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16, P17) => T): Seq[T] =
    select(template)(rs => f(typeMapper.get[P1](rs, 1), typeMapper.get[P2](rs, 2), typeMapper.get[P3](rs, 3), typeMapper.get[P4](rs, 4), typeMapper.get[P5](rs, 5), typeMapper.get[P6](rs, 6), typeMapper.get[P7](rs, 7), typeMapper.get[P8](rs, 8), typeMapper.get[P9](rs, 9), typeMapper.get[P10](rs, 10), typeMapper.get[P11](rs, 11), typeMapper.get[P12](rs, 12), typeMapper.get[P13](rs, 13), typeMapper.get[P14](rs, 14), typeMapper.get[P15](rs, 15), typeMapper.get[P16](rs, 16), typeMapper.get[P17](rs, 17)))

  def select[P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16, P17, P18, T](template: SqlTemplate, f: (P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16, P17, P18) => T): Seq[T] =
    select(template)(rs => f(typeMapper.get[P1](rs, 1), typeMapper.get[P2](rs, 2), typeMapper.get[P3](rs, 3), typeMapper.get[P4](rs, 4), typeMapper.get[P5](rs, 5), typeMapper.get[P6](rs, 6), typeMapper.get[P7](rs, 7), typeMapper.get[P8](rs, 8), typeMapper.get[P9](rs, 9), typeMapper.get[P10](rs, 10), typeMapper.get[P11](rs, 11), typeMapper.get[P12](rs, 12), typeMapper.get[P13](rs, 13), typeMapper.get[P14](rs, 14), typeMapper.get[P15](rs, 15), typeMapper.get[P16](rs, 16), typeMapper.get[P17](rs, 17), typeMapper.get[P18](rs, 18)))

  def select[P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16, P17, P18, P19, T](template: SqlTemplate, f: (P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16, P17, P18, P19) => T): Seq[T] =
    select(template)(rs => f(typeMapper.get[P1](rs, 1), typeMapper.get[P2](rs, 2), typeMapper.get[P3](rs, 3), typeMapper.get[P4](rs, 4), typeMapper.get[P5](rs, 5), typeMapper.get[P6](rs, 6), typeMapper.get[P7](rs, 7), typeMapper.get[P8](rs, 8), typeMapper.get[P9](rs, 9), typeMapper.get[P10](rs, 10), typeMapper.get[P11](rs, 11), typeMapper.get[P12](rs, 12), typeMapper.get[P13](rs, 13), typeMapper.get[P14](rs, 14), typeMapper.get[P15](rs, 15), typeMapper.get[P16](rs, 16), typeMapper.get[P17](rs, 17), typeMapper.get[P18](rs, 18), typeMapper.get[P19](rs, 19)))

  def select[P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16, P17, P18, P19, P20, T](template: SqlTemplate, f: (P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16, P17, P18, P19, P20) => T): Seq[T] =
    select(template)(rs => f(typeMapper.get[P1](rs, 1), typeMapper.get[P2](rs, 2), typeMapper.get[P3](rs, 3), typeMapper.get[P4](rs, 4), typeMapper.get[P5](rs, 5), typeMapper.get[P6](rs, 6), typeMapper.get[P7](rs, 7), typeMapper.get[P8](rs, 8), typeMapper.get[P9](rs, 9), typeMapper.get[P10](rs, 10), typeMapper.get[P11](rs, 11), typeMapper.get[P12](rs, 12), typeMapper.get[P13](rs, 13), typeMapper.get[P14](rs, 14), typeMapper.get[P15](rs, 15), typeMapper.get[P16](rs, 16), typeMapper.get[P17](rs, 17), typeMapper.get[P18](rs, 18), typeMapper.get[P19](rs, 19), typeMapper.get[P20](rs, 20)))

  def select[P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16, P17, P18, P19, P20, P21, T](template: SqlTemplate, f: (P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16, P17, P18, P19, P20, P21) => T): Seq[T] =
    select(template)(rs => f(typeMapper.get[P1](rs, 1), typeMapper.get[P2](rs, 2), typeMapper.get[P3](rs, 3), typeMapper.get[P4](rs, 4), typeMapper.get[P5](rs, 5), typeMapper.get[P6](rs, 6), typeMapper.get[P7](rs, 7), typeMapper.get[P8](rs, 8), typeMapper.get[P9](rs, 9), typeMapper.get[P10](rs, 10), typeMapper.get[P11](rs, 11), typeMapper.get[P12](rs, 12), typeMapper.get[P13](rs, 13), typeMapper.get[P14](rs, 14), typeMapper.get[P15](rs, 15), typeMapper.get[P16](rs, 16), typeMapper.get[P17](rs, 17), typeMapper.get[P18](rs, 18), typeMapper.get[P19](rs, 19), typeMapper.get[P20](rs, 20), typeMapper.get[P21](rs, 21)))

  def select[P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16, P17, P18, P19, P20, P21, P22, T](template: SqlTemplate, f: (P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16, P17, P18, P19, P20, P21, P22) => T): Seq[T] =
    select(template)(rs => f(typeMapper.get[P1](rs, 1), typeMapper.get[P2](rs, 2), typeMapper.get[P3](rs, 3), typeMapper.get[P4](rs, 4), typeMapper.get[P5](rs, 5), typeMapper.get[P6](rs, 6), typeMapper.get[P7](rs, 7), typeMapper.get[P8](rs, 8), typeMapper.get[P9](rs, 9), typeMapper.get[P10](rs, 10), typeMapper.get[P11](rs, 11), typeMapper.get[P12](rs, 12), typeMapper.get[P13](rs, 13), typeMapper.get[P14](rs, 14), typeMapper.get[P15](rs, 15), typeMapper.get[P16](rs, 16), typeMapper.get[P17](rs, 17), typeMapper.get[P18](rs, 18), typeMapper.get[P19](rs, 19), typeMapper.get[P20](rs, 20), typeMapper.get[P21](rs, 21), typeMapper.get[P22](rs, 22)))

  def selectInt(template: SqlTemplate): Option[Int] = {
    selectFirst(template)(_.getInt(1))
  }

  def selectString(template: SqlTemplate): Option[String] = {
    selectFirst(template)(_.getString(1))
  }

  def close(): Unit = conn.close()

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

