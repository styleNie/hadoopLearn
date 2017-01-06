package practice; /**
 * Created by Administrator on 2016/12/21.
 * http://blog.csdn.net/duan_zhihua/article/details/50670195
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.Arrays;

public class TopNSorted2 {

    public static class DataMapper extends Mapper<LongWritable, Text, Text, Text>{
        int[] topN;
        int length;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            length = context.getConfiguration().getInt("topn", 5);
            topN = new int[length + 1];
            for (int i=0;i<length+1;i++) {
                System.out.println("map setup:  " + i +" =i,topN   "+topN[i]);
            }
        }

        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {

            System.out.println("Map Methond Invoked!!!");
            String[] data = value.toString().split(",");
            if (4 == data.length){
                int cost = Integer.valueOf(data[2]);
                topN[0] = cost;
                System.out.println("map:  topN[0]  "+  topN[0]);
                Arrays.sort(topN);
                for (int i=0;i<length + 1;i++) {
                    System.out.println("map:  "+ i +" =i,topN   "+topN[i]);
                }
            }
        }
        @Override
        protected void cleanup(Context context)
                throws IOException, InterruptedException {
            for(int i = 1; i < length + 1; i++){
                context.write(new Text(String.valueOf(topN[i])), new Text(String.valueOf(topN[i])));
            }
        }
    }

    public static class DataReducer extends Reducer<Text,Text,Text, Text> {
        int[] topN;
        int length;

        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {
            length = context.getConfiguration().getInt("topn", 5);
            topN = new int[length + 1];
            for (int i=0;i<length + 1;i++) {
                System.out.println("Reducer setup:  "+ i +" =i,topN   "+topN[i]);
            }
        }

        public void reduce(Text key, Iterable<Text> values, Context context
        ) throws IOException, InterruptedException {
            System.out.println("Reduce Methond Invoked!!!" );

            topN[0] = Integer.valueOf(key.toString());
            System.out.println("reduce:  topN[0]  "+  topN[0]);
            Arrays.sort(topN);
            for (int i=0;i<length+1;i++) {
                System.out.println("reduce:  "+ i +" =i,topN   "+topN[i]);
            }
        }
        @Override
        protected void cleanup(Context context)
                throws IOException, InterruptedException {
            for(int i = length; i > 0; i--){
                System.out.println("reduce cleanup:  "+ i +" =i,topN   "+topN[i]);
                context.write(new Text(String.valueOf(length - i + 1)), new Text(String.valueOf(topN[i])));
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.setInt("topn", 3);
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: TopNSorted <in> [<in>...] <out>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "TopNSorted");
        job.setJarByClass(TopNSorted2.class);
        job.setMapperClass(DataMapper.class);
        job.setReducerClass(DataReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
