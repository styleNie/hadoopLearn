package bigdata.kmeans;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import bigdata.util.DistanceUtil;
import bigdata.util.HadoopCfg;
import bigdata.util.HadoopUtil;

/**
 * K-Means
 *
 * @author wwhhf
 * http://blog.csdn.net/qq_17612199/article/details/51415739
 *
 */
public class KMeansMapReduce {

    private static final String JOB_NAME = "kmeans";
    private static final String RES_PATH = "input\\kmeans_res";
    private static final String POINTS = "kmeans";

    // 中心点 name -> points
    private static Map<String, Vector<Double>> points = new HashMap<String, Vector<Double>>();

    public static void initPoint(String pathin, String filename) throws IOException {
        List<String> lines = HadoopUtil.lsAllFile(pathin, filename);
        for (String line : lines) {
            String terms[] = line.toString().split(":");
            points.put(terms[0], DistanceUtil.getVector(terms[1].split(",")));
        }
    }

    public static class KMeansMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String fileName = fileSplit.getPath().getName();
            if (POINTS.equals(fileName)) {
                String terms[] = value.toString().split(",");
                Vector<Double> p = DistanceUtil.getVector(terms);
                String center = null;
                double minx = Double.MAX_VALUE;
                for (Entry<String, Vector<Double>> entry : points.entrySet()) {
                    try {
                        double dis = DistanceUtil.getEuclideanDisc(entry.getValue(), p);
                        if (dis < minx) {
                            minx = dis;
                            center = entry.getKey();
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                context.write(new Text(center), value);
            }
        }
    }

    public static class KMeansReducer extends Reducer<Text, Text, Text, NullWritable> {

        // 多路输出
        private MultipleOutputs<Text, NullWritable> output = null;

        private static int cnt = 1;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            //多文件输出
            output = new MultipleOutputs<Text, NullWritable>(context);
            cnt++;
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
            output.close();
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            List<Vector<Double>> list = new ArrayList<Vector<Double>>();
            int num = 0;
            for (Text value : values) {
                String terms[] = value.toString().split(",");
                num ++;
                Vector<Double> p = DistanceUtil.getVector(terms);
                list.add(p);
                output.write(
                        new Text(value + " is belong to " + key.toString()),
                        NullWritable.get(), RES_PATH + cnt);
            }
            String point = DistanceUtil.getAvg(list);
            //System.out.println(key.toString()+":"+point);
            context.write(new Text(key.toString() + ":" + point), NullWritable.get());
        }
    }

    public static void solve(String pointsin, String pathout)
            throws ClassNotFoundException, InterruptedException {
        try {
            Configuration cfg = HadoopCfg.getConfiguration();
            Job job = Job.getInstance(cfg);
            job.setJobName(JOB_NAME);
            job.setJarByClass(ClusterMapReduce.class);

            // mapper
            job.setMapperClass(KMeansMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            // reducer
            job.setReducerClass(KMeansReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);
            job.setNumReduceTasks(2);
            FileInputFormat.addInputPath(job, new Path(pointsin));
            FileOutputFormat.setOutputPath(job, new Path(pathout));
            FileOutputFormat.setOutputPath(job, new Path(pathout));

            job.waitForCompletion(true);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws ClassNotFoundException,
            InterruptedException, IOException {
        String path = "input\\kmeans";
        String pathout = "input\\kmeans";
        String tmp_pathin = pathout;
        String point_filename = "part-r-";
        for (int i = 1; i <= 2; i++) {
            initPoint(tmp_pathin, point_filename);
            String tmp_pathout = pathout + i;
            solve(path, tmp_pathout);
            tmp_pathin = tmp_pathout;
        }
    }
}
