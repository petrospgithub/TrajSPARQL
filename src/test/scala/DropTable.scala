import java.sql.DriverManager

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class DropTable extends FunSuite {

  test("Thesis drop tables") {
    val con = DriverManager.getConnection("jdbc:hive2://83.212.100.24:10000/default", "root", "dithesis13@")
    val stmt = con.createStatement

    stmt.execute("DROP TABLE index_oasa400 ")
    stmt.execute("DROP TABLE index_oasa400_binary ")
    stmt.execute("DROP TABLE index_oasa400_binarytraj ")
    stmt.execute("DROP TABLE index_oasa400_temp ")
    stmt.execute("DROP TABLE index_oasa400_temp_binary ")
    stmt.execute("DROP TABLE index_oasa400_temp_binarytraj ")
    stmt.execute("DROP TABLE oasa400_temp ")
    stmt.execute("DROP TABLE oasa400_temp_binary ")
    stmt.execute("DROP TABLE oasa400_temp_binarytraj ")
    stmt.execute("DROP TABLE partition_index_oasa400 ")
    stmt.execute("DROP TABLE partition_index_oasa400_binary ")
    stmt.execute("DROP TABLE partition_index_oasa400_binarytraj ")
    stmt.execute("DROP TABLE  trajectories_oasa400 ")
    stmt.execute("DROP TABLE trajectories_oasa400_binary ")
    stmt.execute("DROP TABLE trajectories_oasa400_binarytraj ")

  }
}
