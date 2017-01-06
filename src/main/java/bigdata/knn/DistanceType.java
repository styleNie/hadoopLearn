package bigdata.knn;

/**
 * Created by Administrator on 2016/12/27.
 * http://blog.csdn.net/qq_17612199/article/details/51416897
 */
public class DistanceType implements Comparable<DistanceType> {

    private double distance;
    private int type;

    public double getDistance() {
        return distance;
    }

    public void setDistance(double distance) {
        this.distance = distance;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public DistanceType(String s) {
        super();
        String terms[] = s.split(":");
        this.type = Integer.valueOf(terms[0]);
        this.distance = Double.valueOf(terms[1]);
    }

    public int compareTo(DistanceType o) {
        return this.getDistance() > o.getDistance() ? 1 : -1;
    }

    @Override
    public String toString() {
        return "DistanceType [distance=" + distance + ", type=" + type + "]";
    }

}
