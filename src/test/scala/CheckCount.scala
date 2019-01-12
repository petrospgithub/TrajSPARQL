import java.sql.{DriverManager, ResultSet}

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner


@RunWith(classOf[JUnitRunner])
class CheckCount  extends FunSuite {

  test("Thesis check count") {

    val con = DriverManager.getConnection("jdbc:hive2://83.212.100.24:10000/default", "root", "dithesis13@")
    val stmt = con.createStatement
    var rs:Option[ResultSet]=None

    val arr = Array[String] (
      "imis400_temp",
        "imis400_temp_binary",
        "index_imis400",
        "index_imis400_binary",
        "index_imis400_binarytraj",
        "index_imis400_temp",
        "index_imis400_temp_binary",
        "index_imis400_temp_binarytraj",
        "partition_index_imis400",
        "partition_index_imis400_binary",
        "trajectories_imis400",
        "trajectories_imis400_binary",
        "trajectories_imis400_binary_pid",
        "trajectories_imis400_pid"
    )

    arr.foreach(f=>{

      rs=Some(stmt.executeQuery("SELECT count(*) FROM "+f))

      if (rs.get.next()) {
        val obj=rs.get.getObject(1)
        println("count "+f+": "+ obj)
      }

      rs=Some(stmt.executeQuery("SELECT * FROM "+f+" LIMIT 1"))

      if (rs.get.next()) {

        //println("metadata: "+rs.get.getMetaData.)

        val obj=rs.get.getObject(1)
        println("row "+f+": "+ obj)
      }

      println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")

    })




    if (rs.isDefined) {
      rs.get.close()
    }
    stmt.close()
    con.close()

  }

}


/*


 */