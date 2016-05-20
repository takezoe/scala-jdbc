# better-jdbc

Better JDBC wrapper for Scala based on [better-files](https://github.com/pathikrit/better-files)'s auto resource management.

You can use better-jdbc by adding a following import statement only:

```scala
import better.jdbc._
```

## Select

Extract values from `ResultSet`:

```scala
val results = for {
  db <- DB(conn).autoClosed
} yield {
  db.select("SELECT * FROM USERS"){ rs =>
    (rs.getInt("USER_ID"), rs.getString("USER_NAME"))
  }
}

val users: Seq[(Int, String)] = results.head
```

Select a first record:

```scala
val results = for {
  db <- DB(conn).autoClosed
} yield {
  db.selectFirst(sql"SELECT * FROM USERS WHERE USER_ID = $userId"){ rs =>
    (rs.getInt("USER_ID"), rs.getString("USER_NAME"))
  }
}

val user: Option[(Int, String)] = results.head
```

Map results to the case class:

```scala
case class User(userId: Int, userName: String)

val results = for {
  db <- DB(conn).autoClosed
} yield {
  db.select("SELECT USER_ID, USER_NAME FROM USERS", User.apply)
}

val users: Seq[User] = results.head
```

## Update

```scala
// Plain string
for {
  db <- DB(conn).autoClosed
} db.update("DELETE FROM USER WHERE USER_ID = 1")

// Embed variables
for {
  db <- DB(conn).autoClosed
} db.update(sql"INSERT INTO USER (USER_ID, USER_NAME) VALUES ($userId, $userName)")
```

## Transaction

TODO

