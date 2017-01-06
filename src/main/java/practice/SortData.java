package practice; /**
 * Created by Administrator on 2016/12/21.
 * http://blog.csdn.net/duan_zhihua/article/details/50658525
 *
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

public class SortData {
    /**
     * 使用Mapper将输入文件中的数据作为Mapp输出的key直接输出
     */
    public static class ForSortDataMapper extends Mapper<Object, Text, LongWritable, LongWritable> {

        private LongWritable data = new LongWritable(1);
        private LongWritable eValue = new LongWritable(1);

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            data.set(Long.valueOf(value.toString()));
            context.write(data, eValue);
        }
    }

    /**
     * 使用Reducer将输入的key本身作为输入的key直接输出
     */
    public static class ForSortReducer
            extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {

        private LongWritable position = new LongWritable(1);

        public void reduce(LongWritable key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {
            for(LongWritable item : values){
                context.write(position, key);
                position.set(position.get() + 1);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        long start = System.currentTimeMillis();
        Configuration conf = new Configuration();

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: Sort <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = new Job(conf, "Sort Data");
        job.setJarByClass(SortData.class);
        job.setMapperClass(ForSortDataMapper.class);
        job.setCombinerClass(ForSortReducer .class);
        job.setReducerClass(ForSortReducer.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(LongWritable.class);
        job.setNumReduceTasks(128);

        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
        //System.exit(job.waitForCompletion(true) ? 0 : 1);
        job.waitForCompletion(true);
        long end = System.currentTimeMillis();
        System.out.println("time cost : "+(end-start)/1000);
    }
}
