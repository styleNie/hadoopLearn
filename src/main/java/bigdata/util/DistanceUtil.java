package bigdata.util;

/**
 * Created by Administrator on 2016/12/27.
 */

import java.util.List;
import java.util.Vector;

public class DistanceUtil {

    public static Vector<Double> getVector(String terms[]){
        Vector<Double> vector = new Vector<Double>();
        for(String term : terms){
            vector.add(Double.valueOf(term));
        }
        return vector;
    }

    public static double getEuclideanDisc(Vector<Double> x, Vector<Double> y) {
        assert x.size() == y.size();
        double result = 0;
        try {
            for (int i = 0; i < x.size(); i++) {
                result += Math.pow(x.get(i) - y.get(i), 2.0);
            }
            return Math.sqrt(result);
        }catch (Exception e) {
            e.printStackTrace();
            return 0;
        }
    }

    public static String getAvg(List<Vector<Double>> list){
        int numList = list.size();
        int numElem = list.get(0).size();
        Double[] result = new Double[numElem];
        for(int i=0;i<numElem;i++){
            result[i] = 0.0;
        }
        for(Vector<Double> vec : list){
            for(int i=0;i<vec.size();i++){
                result[i] += vec.get(i)/numList;
            }
        }
        StringBuffer tmp = new StringBuffer();
        for(Double re:result){
            tmp.append(re + ",");
        }
        return tmp.toString().substring(0, tmp.toString().length() - 1);
    }
}
