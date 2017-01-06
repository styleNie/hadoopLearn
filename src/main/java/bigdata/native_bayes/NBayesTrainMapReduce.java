package bigdata.native_bayes;

/**
 * Created by Administrator on 2016/12/26.
 */
import java.io.IOException;

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

import bigdata.util.HadoopCfg;

public class NBayesTrainMapReduce {

    private static final String Train = "NBayes.train";

    // P(A|B)=P(AB)/P(B)
    public static class NBayesTrainMapper extends
            Mapper<LongWritable, Text, Text, IntWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String fileName = fileSplit.getPath().getName();
            if (Train.equals(fileName)) {
                String terms[] = value.toString().split(" ");
                // P(yi)
                context.write(new Text(terms[0]), new IntWritable(1));
                for (int i = 1, len = terms.length; i < len; i++) {
                    // P(xj|yi)
                    context.write(
                            new Text(terms[0] + ":" + i + ":" + terms[i]),
                            new IntWritable(1));
                }
            }
        }
    }

    public static class NBayesTrainReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    private static final String JOB_NAME = "NB";

    public static void solve(String pathin, String pathout)
            throws ClassNotFoundException, InterruptedException {
        try {
            Configuration cfg = HadoopCfg.getConfiguration();
            Job job = Job.getInstance(cfg);
            job.setJobName(JOB_NAME);
            job.setJarByClass(NBayesTrainMapReduce.class);

            // mapper
            job.setMapperClass(NBayesTrainMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);

            // reducer
            job.setReducerClass(NBayesTrainReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

            FileInputFormat.addInputPath(job, new Path(pathin));
            FileOutputFormat.setOutputPath(job, new Path(pathout));

            job.waitForCompletion(true);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws ClassNotFoundException,
            InterruptedException {
        solve("input\\NBayesTrainMapReduce", "output");
    }
}