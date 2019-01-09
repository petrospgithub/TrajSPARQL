import java.sql.*;

public class HiveJavaClient {
    public static void main (String[] args) throws SQLException {
        Connection con = DriverManager.getConnection(
                "jdbc:hive2://83.212.100.24:10000/default", "root", "dithesis13@");
        Statement stmt = con.createStatement();


        // show tables
        String sql = "show tables";
        System.out.println("Running: " + sql);
        ResultSet res = stmt.executeQuery(sql);
        while (res.next()) {
            System.out.println(res.getString(1));
        }

        res.close();
        stmt.close();
        con.close();
    }
}
