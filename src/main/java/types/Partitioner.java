package types;

import scala.Product;
import scala.Serializable;

public abstract class Partitioner implements Serializable, Product {

    private long id;
    private CPointST[] trajectory;
    private long rowId;
    private long pid;

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public CPointST[] getTrajectory() {
        return trajectory;
    }

    public void setTrajectory(CPointST[] trajectory) {
        this.trajectory = trajectory;
    }

    public long getRowId() {
        return rowId;
    }

    public void setRowId(long rowId) {
        this.rowId = rowId;
    }

    public long getPid() {
        return pid;
    }

    public void setPid(long aLong) {
        pid = aLong;
    }

    public Partitioner(long id, CPointST[] trajectory, long rowId, long pid) {
        this.id = id;
        this.trajectory = trajectory;
        this.rowId = rowId;
        this.pid = pid;
    }

    public MbbST mbbST() {
        long min_t = trajectory[0].getTimestamp();

        int length=trajectory.length;

        long max_t = trajectory[length-1].getTimestamp();

        int i = 1;

        double newMinX = trajectory[0].getLongitude();
        double newMaxX = trajectory[0].getLongitude();

        double newMinY = trajectory[0].getLatitude();
        double newMaxY = trajectory[0].getLatitude();

        while (i < trajectory.length) {

            newMinX = Math.min(trajectory[i].getLongitude(), newMinX);
            newMaxX = Math.max(trajectory[i].getLongitude(), newMaxX);
            newMinY = Math.min(trajectory[i].getLatitude(), newMinY);
            newMaxY = Math.max(trajectory[i].getLatitude(), newMaxY);

            i = i + 1;
        }

        MbbST ret=new MbbST(id, newMinX, newMaxX, newMinY, newMaxY, min_t, max_t);

        ret.setGid(id);
        //println(ret)
        return ret;
    }

}
