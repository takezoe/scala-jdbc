package com.github.takezoe.scala.jdbc.validation

import org.scalatest.FunSuite
import com.github.takezoe.scala.jdbc._

class SqlValidationSpec extends FunSuite {

  test("macro test"){

    sqlc("SELECT A.USER_ID, A.USER_NAME, B.COMPANY_NAME, C.DEP_NAME " +
      "FROM USER A, (SELECT DEPT_ID, DEPT_NAME FROM DEPT) C " +
      "INNER JOIN COMPANY B ON A.COMPANY_ID = B.COMPANY_ID " +
      "WHERE B.COMPANY_ID = 1 AND C.DEPT_ID = A.DEPT_ID " +
      "ORDER BY A.USER_ID")
  }

}