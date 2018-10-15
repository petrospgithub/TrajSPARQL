package types;

import types.CPointST;
import types.MbbST;

public abstract class Partitioner {

    private long id;
    private CPointST[] trajectory;
    private long rowId;
    private long Long;

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
