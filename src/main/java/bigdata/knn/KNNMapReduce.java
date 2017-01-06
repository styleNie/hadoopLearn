package bigdata.knn;

/**
 * Created by Administrator on 2016/12/27.
 * http://blog.csdn.net/qq_17612199/article/details/51416897
 */
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import bigdata.util.DistanceUtil;
import bigdata.util.HadoopCfg;
import bigdata.util.HadoopUtil;

public class KNNMapReduce {

    public static final String POINTS = "knn-test";
    public static final int K = 3;
    public static final int TYPES = 3;
    private static final String JOB_NAME = "knn";

    // train-points
    private static List<Point> trans_points = new ArrayList<Point>();

    public static void initPoints(String pathin, String filename)
            throws IOException {
        List<String> lines = HadoopUtil.lsAllFile(pathin, filename);
        for (String line : lines) {
            trans_points.add(new Point(line));
        }
    }

    public static class KNNMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String fileName = fileSplit.getPath().getName();
            if (POINTS.equals(fileName)) {
                Point point1 = new Point(value.toString());
                try {
                    for (Point point2 : trans_points) {
                        double dis = DistanceUtil.getEuclideanDisc(point1.getV(), point2.getV());
                        context.write(new Text(point1.toString()), new Text(point2.getType() + ":" + dis));
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static class KNNReducer extends Reducer<Text, Text, Text, IntWritable> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            List<DistanceType> list = new ArrayList<DistanceType>();
            for (Text value : values) {
                list.add(new DistanceType(value.toString()));
            }
            Collections.sort(list);
            int cnt[] = new int[TYPES + 1];
            for (int i = 0, len = cnt.length; i < len; i++) {
                cnt[i] = 0;
            }
            for (int i = 0; i < K; i++) { // 取前K个邻居计数
                cnt[list.get(i).getType()]++;
            }
            int type = 0;
            int maxx = Integer.MIN_VALUE;
            for (int i = 1; i <= TYPES; i++) {
                if (cnt[i] > maxx) {
                    maxx = cnt[i];
                    type = i;
                }
            }
            context.write(key, new IntWritable(type));
        }
    }

    public static void solve(String pointin, String pathout) throws ClassNotFoundException, InterruptedException {
        try {
            Configuration cfg = HadoopCfg.getConfiguration();
            Job job = Job.getInstance(cfg);
            job.setJobName(JOB_NAME);
            job.setJarByClass(KNNMapReduce.class);

            // mapper
            job.setMapperClass(KNNMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            // reducer
            job.setReducerClass(KNNReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

            FileInputFormat.addInputPath(job, new Path(pointin));
            FileOutputFormat.setOutputPath(job, new Path(pathout));

            job.waitForCompletion(true);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws ClassNotFoundException, InterruptedException, IOException {
        initPoints("input\\knn", "knn-train");
        solve("input\\knn", "knn_out");
    }
}
