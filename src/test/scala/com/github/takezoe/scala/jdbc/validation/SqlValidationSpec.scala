package com.github.takezoe.scala.jdbc.validation

import org.scalatest.FunSuite
import com.github.takezoe.scala.jdbc._

class SqlValidationSpec extends FunSuite {

  test("macro test"){
    val companyId = 1
    val companyName = "xxx"

    val s = sqlc(s"""
      |SELECT
      |  A.USER_ID,
      |  A.USER_NAME,
      |  B.COMPANY_NAME,
      |  C.DEPT_NAME
      |FROM
      |  USER A,
      |  (SELECT DEPT_ID, DEPT_NAME FROM DEPT) C
      |INNER JOIN COMPANY B ON A.COMPANY_ID = B.COMPANY_ID
      |WHERE
      |  B.COMPANY_ID   = ${companyId}   AND
      |  B.COMPANY_NAME = ${companyName} AND
      |  C.DEPT_ID      = A.DEPT_ID      AND
      |  C.DEPT_ID IN (SELECT DEPT_ID FROM DEPT_GROUP)
      |ORDER BY A.USER_ID
    """.stripMargin)

    println(s)
  }

}
