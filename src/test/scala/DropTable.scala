import java.sql.DriverManager

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class DropTable extends FunSuite {

  test("Thesis drop tables") {
    val con = DriverManager.getConnection("jdbc:hive2://83.212.100.24:10000/default", "root", "dithesis13@")
    val stmt = con.createStatement

    stmt.execute("DROP TABLE imis400_temp ")
    stmt.execute("DROP TABLE imis400_temp_binary ")
    stmt.execute("DROP TABLE index_imis400 ")
    stmt.execute("DROP TABLE index_imis400_binary ")
    stmt.execute("DROP TABLE index_imis400_binarytraj ")
    stmt.execute("DROP TABLE index_imis400_temp ")
    stmt.execute("DROP TABLE index_imis400_temp_binary ")
    stmt.execute("DROP TABLE index_imis400_temp_binarytraj ")
    stmt.execute("DROP TABLE partition_index_imis400 ")
    stmt.execute("DROP TABLE partition_index_imis400_binary ")
    stmt.execute("DROP TABLE trajectories_imis400 ")
    stmt.execute("DROP TABLE trajectories_imis400_binary ")
    stmt.execute("DROP TABLE trajectories_imis400_binary_pid ")
    stmt.execute("DROP TABLE trajectories_imis400_pid ")


  }
}
