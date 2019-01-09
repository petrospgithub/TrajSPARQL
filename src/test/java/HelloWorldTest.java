import di.thesis.indexing.types.EnvelopeST;
import junit.framework.TestCase;
import utils.MbbSerialization;

import java.sql.*;
import java.util.ArrayList;

//mvn test -Dtest=HelloWorldTest -q -DargLine="-Dbuckets=100"

public class HelloWorldTest extends TestCase {
    private Connection con = DriverManager.getConnection("jdbc:hive2://83.212.100.24:10000/default", "root", "dithesis13@");
    private Statement stmt = con.createStatement();
    private ResultSet rs = null;
    private EnvelopeST env = null;

    public HelloWorldTest() throws SQLException {
    }


    public double sum(ArrayList<Long> foo) {

        double sum = 0.0;

        for (int i = 0; i < foo.size(); i++) {
            sum = sum + foo.get(i);
        }
        return sum;
    }

    public Long rangeArrStruct_BF() throws SQLException {

        rs = stmt.executeQuery("select box from index_imis400_binary distribute by rand() sort by rand() limit 1");

        while (rs.next()) {
            Object obj = rs.getObject(1);

            env = MbbSerialization.deserialize((byte[]) obj);

        }

        long start = System.currentTimeMillis();

        String sql = String.format(" SELECT rowId " +
                        " FROM imis400_temp " +
                        " WHERE ST_Intersects3DBinary(MbbConstructorBinary( %s, %s, %s, %s, CAST(%s as BIGINT), CAST(%s as BIGINT) ), trajectory, 0.1, 0.1, 0.1, 0.1, 0, 0) ",
                env.getMinX(), env.getMaxX(), env.getMinY(), env.getMaxY(), env.getMinT(), env.getMaxT());


        stmt.execute(sql);
        return System.currentTimeMillis() - start;
    }

    public Long rangeArrStruct_index() throws SQLException {

        rs = stmt.executeQuery("select box from index_imis400_binary distribute by rand() sort by rand() limit 1");

        while (rs.next()) {
            Object obj = rs.getObject(1);

            env = MbbSerialization.deserialize((byte[]) obj);

        }

        long start = System.currentTimeMillis();

        String sql = String.format(" SELECT * FROM trajectories_imis400 as a INNER JOIN " +
                        " (SELECT ST_IndexIntersects(MbbConstructor( %s, %s, %s, %s, CAST(%s as BIGINT), CAST(%s as BIGINT) ),tree, 0.1, 0.1, 0.1, 0.1, 0, 0) " +
                        " FROM index_imis400 ) as b" +
                        "  ON (b.trajectory_id=a.rowId) " +
                        " WHERE ST_Intersects3D(MbbConstructor( %s, %s, %s, %s, CAST(%s as BIGINT), CAST(%s as BIGINT) ), trajectory, 0.1, 0.1, 0.1, 0.1, 0, 0)  ",
                env.getMinX(), env.getMaxX(), env.getMinY(), env.getMaxY(), env.getMinT(), env.getMaxT(),
                env.getMinX(), env.getMaxX(), env.getMinY(), env.getMaxY(), env.getMinT(), env.getMaxT()
        );

        stmt.execute(sql);
        return System.currentTimeMillis() - start;
    }

    public Long rangeArrStruct_index_pid() throws SQLException {

        rs = stmt.executeQuery("select box from index_imis400_binary distribute by rand() sort by rand() limit 1");

        while (rs.next()) {
            Object obj = rs.getObject(1);

            env = MbbSerialization.deserialize((byte[]) obj);

        }

        long start = System.currentTimeMillis();

        String sql = String.format(" SELECT * FROM trajectories_imis400_pid as a INNER JOIN " +
                        "(SELECT ST_IndexIntersects(MbbConstructor( %s, %s, %s, %s, CAST(%s as BIGINT), CAST(%s as BIGINT) ),tree, 0.1, 0.1, 0.1, 0.1, 0, 0) FROM partition_index_imis400) as b ON (b.trajectory_id=a.pid) " +
                        " WHERE ST_Intersects3D(MbbConstructor( %s, %s, %s, %s, CAST(%s as BIGINT), CAST(%s as BIGINT) ), trajectory, 0.1, 0.1, 0.1, 0.1, 0, 0)  ",
                env.getMinX(), env.getMaxX(), env.getMinY(), env.getMaxY(), env.getMinT(), env.getMaxT(),
                env.getMinX(), env.getMaxX(), env.getMinY(), env.getMaxY(), env.getMinT(), env.getMaxT()
        );

        stmt.execute(sql);
        return System.currentTimeMillis() - start;
    }

    public Long rangeBinary_BF() throws SQLException {

        rs = stmt.executeQuery("select box from index_imis400_binary distribute by rand() sort by rand() limit 1");

        while (rs.next()) {
            Object obj = rs.getObject(1);

            env = MbbSerialization.deserialize((byte[]) obj);

        }


        long start = System.currentTimeMillis();

        String sql = String.format(" SELECT rowId " +
                        " FROM imis400_temp_binary " +
                        " WHERE ST_Intersects3DBinary(MbbConstructorBinary( %s, %s, %s, %s, CAST(%s as BIGINT), CAST(%s as BIGINT) ), trajectory, 0.1, 0.1, 0.1, 0.1, 0, 0) ",
                env.getMinX(), env.getMaxX(), env.getMinY(), env.getMaxY(), env.getMinT(), env.getMaxT());

        stmt.execute(sql);
        return System.currentTimeMillis() - start;
    }

    public Long rangeBinary_index() throws SQLException {
        rs = stmt.executeQuery("select box from index_imis400_binary distribute by rand() sort by rand() limit 1");

        while (rs.next()) {
            Object obj = rs.getObject(1);

            env = MbbSerialization.deserialize((byte[]) obj);

        }
        long start = System.currentTimeMillis();

        String sql = String.format(" SELECT * FROM trajectories_imis400_binary as a INNER JOIN " +
                        " (SELECT ST_IndexIntersectsBinary(MbbConstructorBinary( %s, %s, %s, %s, CAST(%s as BIGINT), CAST(%s as BIGINT) ), tree, 0.1, 0.1, 0.1, 0.1, 0, 0) " +
                        " FROM index_imis400_binary ) as b " +
                        " ON (b.trajectory_id=a.rowId) " +
                        " WHERE ST_Intersects3DBinary(MbbConstructorBinary( %s, %s, %s, %s, CAST(%s as BIGINT), CAST(%s as BIGINT) ), trajectory, 0.1, 0.1, 0.1, 0.1, 0, 0)  ",
                env.getMinX(), env.getMaxX(), env.getMinY(), env.getMaxY(), env.getMinT(), env.getMaxT(),
                env.getMinX(), env.getMaxX(), env.getMinY(), env.getMaxY(), env.getMinT(), env.getMaxT()
        );

        stmt.execute(sql);
        return System.currentTimeMillis() - start;
    }

    public Long rangeBinary_index_pid() throws SQLException {

        rs = stmt.executeQuery("select box from index_imis400_binary distribute by rand() sort by rand() limit 1");

        while (rs.next()) {
            Object obj = rs.getObject(1);

            env = MbbSerialization.deserialize((byte[]) obj);

        }

        long start = System.currentTimeMillis();

        String sql = String.format(" SELECT * FROM trajectories_imis400_binary_pid as a INNER JOIN " +
                        "(SELECT ST_IndexIntersectsBinary(MbbConstructorBinary( %s, %s, %s, %s, CAST(%s as BIGINT), CAST(%s as BIGINT) ),tree, 0.1, 0.1, 0.1, 0.1, 0, 0) FROM partition_index_imis400_binary) as b ON (b.trajectory_id=a.pid) " +
                        " WHERE ST_Intersects3DBinary(MbbConstructorBinary( %s, %s, %s, %s, CAST(%s as BIGINT), CAST(%s as BIGINT) ), trajectory, 0.1, 0.1, 0.1, 0.1, 0, 0)  ",
                env.getMinX(), env.getMaxX(), env.getMinY(), env.getMaxY(), env.getMinT(), env.getMaxT(),
                env.getMinX(), env.getMaxX(), env.getMinY(), env.getMaxY(), env.getMinT(), env.getMaxT()
        );

        stmt.execute(sql);
        return System.currentTimeMillis() - start;
    }

    public Long rangeBinaryTraj() throws SQLException {

        rs = stmt.executeQuery("select box from index_imis400_binary distribute by rand() sort by rand() limit 1");

        while (rs.next()) {
            Object obj = rs.getObject(1);

            env = MbbSerialization.deserialize((byte[]) obj);

        }


        long start = System.currentTimeMillis();

        String sql = String.format(" SELECT IndexIntersectsTraj(MbbConstructorBinary( %s, %s, %s, %s, CAST(%s as BIGINT), CAST(%s as BIGINT) ), tree, 0.1, 0.1, 0.1, 0.1, 0, 0) FROM index_imis400_temp_binaryTraj ",
                env.getMinX(), env.getMaxX(), env.getMinY(), env.getMaxY(), env.getMinT(), env.getMaxT());

        stmt.execute(sql);
        return System.currentTimeMillis() - start;
    }

    public void test() throws Exception {

     //stmt.execute(" ADD JAR hdfs:///user/root/hiveThesis/HiveTrajSPARQL-jar-with-dependencies.jar ");

        stmt.execute(" SET hive.auto.convert.join=true ");
        stmt.execute(" SET hive.enforce.bucketing=true ");
        stmt.execute(" SET hive.optimize.bucketmapjoin.sortedmerge = true ");
        stmt.execute(" SET hive.auto.convert.sortmerge.join=true ");
        stmt.execute(" SET hive.optimize.bucketmapjoin = true ");
        stmt.execute(" SET hive.auto.convert.join.noconditionaltask = true ");
        stmt.execute(" SET hive.auto.convert.join.noconditionaltask.size = 10000000 ");

        stmt.execute(" SET hive.vectorized.execution.enabled=true ");
        stmt.execute(" SET hive.exec.parallel=true ");
        stmt.execute(" SET mapred.compress.map.output=true ");
        stmt.execute(" SET mapred.output.compress=true ");
        stmt.execute(" SET hive.cbo.enable=true ");
        stmt.execute(" SET hive.stats.autogather=true ");
        stmt.execute(" SET hive.optimize.ppd=true ");
        stmt.execute(" SET hive.optimize.ppd.storage=true ");
        stmt.execute(" SET hive.vectorized.execution.reduce.enabled=true ");
        stmt.execute(" SET hive.stats.fetch.column.stats=true ");
        stmt.execute(" SET hive.tez.auto.reducer.parallelism=true ");

        stmt.execute(" set hive.server2.tez.initialize.default.sessions=true ");
        stmt.execute(" set hive.prewarm.enabled=true ");
        stmt.execute(" set hive.prewarm.numcontainers=15 ");
        stmt.execute(" set tez.am.container.reuse.enabled=true ");
        stmt.execute(" set hive.server2.enable.doAs=false ");


        stmt.execute(" CREATE TEMPORARY FUNCTION ST_Intersects3D AS 'di.thesis.hive.stoperations.ST_Intersects3D' ");
        stmt.execute(" CREATE TEMPORARY FUNCTION MbbConstructor AS 'di.thesis.hive.mbb.MbbSTUDF' ");
        stmt.execute(" CREATE TEMPORARY FUNCTION ST_IndexIntersects AS 'di.thesis.hive.stoperations.IndexIntersects3D' ");


        stmt.execute(" CREATE TEMPORARY FUNCTION ST_Intersects3DBinary AS 'di.thesis.hive.stoperations.ST_Intersects3DBinary' ");
        stmt.execute(" CREATE TEMPORARY FUNCTION MbbConstructorBinary AS 'di.thesis.hive.mbb.MbbSTUDFBinary' ");
        stmt.execute(" CREATE TEMPORARY FUNCTION ST_IndexIntersectsBinary AS 'di.thesis.hive.stoperations.IndexIntersects3DBinary' ");

        stmt.execute(" CREATE TEMPORARY FUNCTION IndexIntersectsTraj AS 'di.thesis.hive.stoperations.IndexIntersectsTraj' ");

        ArrayList buffer_rangeArrStructBF = new ArrayList();
        ArrayList buffer_rangeArrStruct_INDEX = new ArrayList();
        ArrayList buffer_rangeArrStruct_PID = new ArrayList();

        ArrayList buffer_rangeBinary_BF = new ArrayList();
        ArrayList buffer_rangeBinary_INDEX = new ArrayList();
        ArrayList buffer_rangeBinary_PID = new ArrayList();


        ArrayList buffer_rangeBinaryTraj = new ArrayList();

        int i = 0;
/*
        while (i < 3) {

            buffer_rangeArrStructBF.add(rangeArrStruct_BF());

            i = i + 1;
        }

        i = 0;

        while (i < 3) {

            buffer_rangeArrStruct_INDEX.add(rangeArrStruct_index());

            i = i + 1;
        }

        i = 0;

        while (i < 3) {

            buffer_rangeArrStruct_PID.add(rangeArrStruct_index_pid());

            i = i + 1;
        }


        i = 0;

        while (i < 3) {

            buffer_rangeBinary_BF.add(rangeBinary_BF());

            i = i + 1;

        }

        i = 0;

        while (i < 3) {

            buffer_rangeBinary_INDEX.add(rangeBinary_index());

            i = i + 1;

        }

        i = 0;

        while (i < 3) {

            buffer_rangeBinary_PID.add(rangeBinary_index_pid());

            i = i + 1;
        }

        i = 0;
*/
        while (i < 1) {

            buffer_rangeBinaryTraj.add(rangeBinaryTraj());

            i = i + 1;

        }

        System.out.println("Arr struct mean time: " + sum(buffer_rangeArrStructBF) / (double) buffer_rangeArrStructBF.size());
        System.out.println("Arr struct index mean time: " + sum(buffer_rangeArrStruct_INDEX) / (double) buffer_rangeArrStruct_INDEX.size());
        System.out.println("Arr struct index pid: " + sum(buffer_rangeArrStruct_PID) / (double) buffer_rangeArrStruct_PID.size());

        System.out.println("Binary mean time: " + sum(buffer_rangeBinary_BF) / (double) buffer_rangeBinary_BF.size());
        System.out.println("Binary index mean time: " + sum(buffer_rangeBinary_INDEX) / (double) buffer_rangeBinary_INDEX.size());
        System.out.println("Binary index pid: " + sum(buffer_rangeBinary_PID) / (double) buffer_rangeBinary_PID.size());

        System.out.println("Binary traj mean time: " + sum(buffer_rangeBinaryTraj) / (double) buffer_rangeBinaryTraj.size());
    }
}
