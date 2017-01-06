package bigdata.kmeans;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import bigdata.util.HadoopCfg;

/**
 * 计算种子点
 *
 * @author wwhhf
 * http://blog.csdn.net/qq_17612199/article/details/51415739
 *
 */
public class ClusterMapReduce {

    public static class ClusterMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String terms[] = value.toString().split(",");
            for (int i = 0, len = terms.length; i < len; i++) {
                context.write(new Text("c" + (i + 1)), new DoubleWritable(Double.valueOf(terms[i])));
            }
        }
    }

    public static class ClusterReducer extends Reducer<Text, DoubleWritable, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            double maxx = Integer.MIN_VALUE;
            double minx = Integer.MAX_VALUE;
            for (DoubleWritable value : values) {
                maxx = Math.max(maxx, value.get());
                minx = Math.min(minx, value.get());
            }
            context.write(key, new Text("" + maxx + "," + minx + "," + ((maxx + minx) * 1.0 / 2)));
        }
    }

    public static class PointMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String terms[] = value.toString().split("\t");
            String values[] = terms[1].split(",");
            double maxx = Double.valueOf(values[0]);
            double minx = Double.valueOf(values[1]);
            double avg = Double.valueOf(values[2]);
            //System.out.println(key.toString());
            context.write(new Text("max"), new Text(key.toString() + "," + maxx));
            context.write(new Text("min"), new Text(key.toString() + "," + minx));
            context.write(new Text("avg"), new Text(key.toString() + "," + avg));
        }
    }

    public static class PointReducer extends Reducer<Text, Text, Text, NullWritable> {

        private static int cnt = 0;

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            TreeMap<String, Double> map = new TreeMap<String, Double>();
            for (Text value : values) {
                // value:c1,5
                String terms[] = value.toString().split(",");
                map.put(terms[0], Double.valueOf(terms[1]));
            }
            // write
            cnt++;
            StringBuffer sb = new StringBuffer();
            for (Entry<String, Double> entry : map.entrySet()) {
                //System.out.println(entry.getKey() + " " + entry.getValue());
                sb.append(entry.getValue()).append(",");
            }
            context.write(
                    new Text("c"
                            + cnt
                            + ":"
                            + sb.toString().substring(0,
                            sb.toString().length() - 1)),
                    NullWritable.get());
        }
    }

    private static final String JOB_NAME = "cluster";

    public static void solve(String pathin, String pathout)
            throws ClassNotFoundException, InterruptedException {
        try {
            Configuration cfg = HadoopCfg.getConfiguration();
            Job job = Job.getInstance(cfg);
            job.setJobName(JOB_NAME);
            job.setJarByClass(ClusterMapReduce.class);

            // mapper
            job.setMapperClass(ClusterMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(DoubleWritable.class);

            // Reducer
            job.setReducerClass(ClusterReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);

            FileInputFormat.addInputPath(job, new Path(pathin));
            FileOutputFormat.setOutputPath(job, new Path(pathout));

            job.waitForCompletion(true);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void genPoint(String pathin, String pathout)
            throws ClassNotFoundException, InterruptedException {
        try {
            Configuration cfg = HadoopCfg.getConfiguration();
            Job job = Job.getInstance(cfg);
            job.setJobName(JOB_NAME);
            job.setJarByClass(ClusterMapReduce.class);

            // mapper
            job.setMapperClass(PointMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            // reducer
            job.setReducerClass(PointReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);

            FileInputFormat.addInputPath(job, new Path(pathin));
            FileOutputFormat.setOutputPath(job, new Path(pathout));

            job.waitForCompletion(true);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws ClassNotFoundException, InterruptedException {
        solve("input\\kmeans\\kmeans", "input\\kmeans_out");
        genPoint("input\\kmeans_out", "input\\kmeans_out1");
    }
}
