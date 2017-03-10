package example

import java.sql.{Connection, DriverManager, ResultSet}

case class PgCon(host: String, port: Int, db: String, user: String, auth: String) {

  val con_str = "jdbc:postgresql://" + host + ":" + port + "/" + db + "?user=" + user
  val conn = DriverManager.getConnection(con_str)
  val stm = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
 
  def runSQL(sql: String) {
    val rs = stm.executeQuery(sql)
    val metadata = rs.getMetaData()
    val noColumns: Int = metadata.getColumnCount()
    while (rs.next) {
      for (col <- 1 until noColumns+1) {
        if (col > 1) {
          print("\t")
        }
        print(rs.getString(col))
      }
      print("\n")
    }
    rs.close
  }

  def executeSQL(sql: String) {
    stm.executeQuery(sql)
  }

  def closeConnection() {
    stm.close()
    conn.close()
  }
}
