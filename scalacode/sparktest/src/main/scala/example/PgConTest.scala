package example

import java.sql.{Connection, DriverManager, ResultSet}

object PgConTest extends App {

  // classOf[org.postgresql.Driver]

  val con_str = "jdbc:postgresql://localhost:5433/rupen?user=postgres"
  val conn = DriverManager.getConnection(con_str)
  try {
    val stm = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)

    val rs = stm.executeQuery("select * from word_counts")

    while(rs.next) {
      println(rs.getString("word") + "\t" + rs.getInt("cnt"))
    }
  } 
  finally {
     conn.close()
  }

  // Using a class to handle connections

  val ct = new PgCon("localhost", 5433, "rupen", "postgres", "")
  ct.runSQL("select * from word_counts order by 1")
  ct.closeConnection
}
