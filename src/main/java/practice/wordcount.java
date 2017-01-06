package practice; /**
 * Created by Administrator on 2016/12/21.
 */
/**
 * Created by Administrator on 2016/12/20.
 */

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class wordcount {

    public static class WordCountMapper extends Mapper<LongWritable,Text,Text,IntWritable> {

        private final IntWritable one = new IntWritable(1);
        private Text word = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()){
                word.set(itr.nextToken());
                context.write(word,one);
            }
        }
    }

    public static class WordCountReducer extends Reducer<Text,IntWritable,IntWritable,Text> {

        private IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for(IntWritable val:values){
                sum += val.get();
            }

            result.set(sum);
            context.write(result,key);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // write your code here
        long start = System.currentTimeMillis();
        Configuration configuration = new Configuration();
        configuration.set("mapred.job.tracker", "localhost:9001");

        if(args.length!=2){
            System.err.println("Usage:practice.wordcount <input><output>");
            System.exit(2);
        }

        Job job = new Job(configuration,"word count");

        job.setJarByClass(wordcount.class);
        job.setMapperClass(WordCountMapper.class);
        //job.setCombinerClass(WordCountReducer.class);
        job.setReducerClass(WordCountReducer.class);
        job.setNumReduceTasks(1);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        //System.exit(job.waitForCompletion(true)?0:1);
        job.waitForCompletion(true);
        long end = System.currentTimeMillis();
        System.out.println("time cost : "+(end-start)/1000);
    }
}