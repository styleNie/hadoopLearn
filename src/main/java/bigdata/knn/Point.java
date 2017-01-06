package bigdata.knn;

/**
 * Created by Administrator on 2016/12/27.
 * http://blog.csdn.net/qq_17612199/article/details/51416897
 */
import java.util.Vector;

public class Point {

    private int type;
    private String strpoint;
    private Vector<Double> v = new Vector<Double>();

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public Vector<Double> getV() {
        return v;
    }

    public void setV(Vector<Double> v) {
        this.v = v;
    }

    public Point(String s) {
        super();
        this.strpoint = s.substring(0,s.length()-2);
        String terms[]=s.split(" ");
        for(int i=0,len=terms.length;i<len-1;i++){
            this.v.add(Double.valueOf(terms[i]));
        }
        this.type=Integer.valueOf(terms[terms.length-1]);
    }

    public Point() {
        super();
    }

    @Override
    public String toString() {
        return this.strpoint;
    }

}