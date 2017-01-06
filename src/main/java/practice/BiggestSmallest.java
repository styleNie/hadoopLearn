package practice; /**
 * Created by Administrator on 2016/12/21.
 * http://blog.csdn.net/duan_zhihua/article/details/50658477
 */
import java.io.IOException;

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

public class BiggestSmallest {
    /*
     * 将文件中的每行数据作为输出的key
     */
    public static class BiggestSmallestMapper extends Mapper<Object, Text, Text,LongWritable>{

        private final LongWritable data = new LongWritable(0);
        private final Text keyFoReducer = new Text("keyFoReducer");

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            System.out.println("Map Method Invoked!");

            data.set(Long.parseLong(value.toString()));
            System.out.println(data);
            context.write(keyFoReducer,data);
        }
    }

    /*
     * 将key输出
     */
    public static  class BiggestSmallestReducer extends Reducer<Text, LongWritable,Text,LongWritable> {

        private long maxValue = Long.MIN_VALUE;
        private long minValue = Long.MAX_VALUE;

        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            System.out.println("Reduce Method Invoked!");

            for(LongWritable item : values){
                if (item.get() > maxValue){
                    maxValue = item.get();
                }
                if (item.get() < minValue){
                    minValue = item.get();
                }
            }
            context.write(new Text("maxValue"), new LongWritable(maxValue));
            //context.write(new Text("minValue"), new LongWritable(minValue));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: DefferentData <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = new Job(conf, "practice.Average");
        job.setJarByClass(BiggestSmallest.class);
        job.setMapperClass(BiggestSmallestMapper.class);
        job.setCombinerClass(BiggestSmallestReducer.class); //加快效率
        job.setReducerClass(BiggestSmallestReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        System.exit(job.waitForCompletion(true)?0:1);
    }
}
