package practice;
/**
 * Created by Administrator on 2016/12/21.
 * http://blog.csdn.net/duan_zhihua/article/details/50753713
 */

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class ChainMapperReducer {

    public static class ChaintDataMapper1 extends Mapper<LongWritable, Text, Text, IntWritable> {

        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {

            System.out.println("ChaintDataMapper1 Methond Invoked!!!");
            String line = value.toString().trim();
            if (line.length() > 1) {
                String[] splited = line.split(",");
                int price = Integer.valueOf(splited[1]);
                if (price < 10000) {
                    context.write(new Text(splited[0].trim()), new IntWritable(price));
                }
            }
        }
    }

    public static class ChaintDataMapper2 extends Mapper<Text, IntWritable, Text, IntWritable> {

        public void map(Text key, IntWritable value, Context context
        ) throws IOException, InterruptedException {

            System.out.println("ChaintDataMapper2  Methond Invoked!!!");
            if (value.get() > 100) {
                context.write(key, value);
            }
        }
    }

    public static class ChaintDataMapper3 extends Mapper<Text, IntWritable, Text, IntWritable> {

        public void map(Text key, IntWritable value, Context context
        ) throws IOException, InterruptedException {

            System.out.println("ChaintDataMapper3  Methond Invoked!!!");
            if (value.get() > 5000) {
                context.write(key, value);
            }
        }
    }

    public static class ChainDataReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context context
        ) throws IOException, InterruptedException {
            System.out.println("Reduce Methond Invoked!!!");

            int summary = 0;
            for (IntWritable item : values) {
                summary += item.get();
            }
            context.write(key, new IntWritable(summary));
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: practice.ChainMapperReducer <in> [<in>...] <out>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "practice.ChainMapperReducer");
        job.setJarByClass(ChainMapperReducer.class);

        job.setMapperClass(ChaintDataMapper1.class);
        job.setReducerClass(ChainDataReducer.class);
        ChainMapper.addMapper(job, ChaintDataMapper1.class, LongWritable.class, Text.class, Text.class, IntWritable.class, new Configuration());
        ChainMapper.addMapper(job, ChaintDataMapper2.class, Text.class, IntWritable.class, Text.class, IntWritable.class, new Configuration());
        ChainReducer.setReducer(job, ChainDataReducer.class, Text.class, IntWritable.class, Text.class, IntWritable.class, new Configuration());
        ChainReducer.addMapper(job, ChaintDataMapper3.class, Text.class, IntWritable.class, Text.class, IntWritable.class, new Configuration());

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
