package practice; /**
 * Created by Administrator on 2016/12/21.
 * http://blog.csdn.net/duan_zhihua/article/details/50658473
 */

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Average {

    public static class DataMapper extends Mapper<Object, Text, Text, FloatWritable> {

        //        public void map(Object key, Text value, Context context
//        ) throws IOException, InterruptedException {
//        用于 key,value类型数据的计算
//            System.out.println("Map Method Invoked!");
//
//            String data = value.toString();
//            System.out.println(data);
//            StringTokenizer splited = new  StringTokenizer(data,"\n");
//            while (splited.hasMoreElements()){
//                StringTokenizer record = new StringTokenizer(splited.nextToken());
//                String name = record.nextToken();
//                String score = record.nextToken();
//                context.write(new Text(name),new FloatWritable(Float.valueOf(score)));
//            }
//        }
        private final FloatWritable data = new FloatWritable(0);
        private final Text keyFoReducer = new Text("keyFoReducer");

        public void map(Object key, Text value, Context content) throws IOException, InterruptedException {
            // 用于没有 key 的数据
            System.out.println("Map Method Invoked!");
            data.set(Long.parseLong(value.toString()));
            System.out.println(data);
            content.write(keyFoReducer, data);
            System.out.println("Map Method Invoked!");
        }
    }

    public static class DataReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {

        public void reduce(Text key, Iterable<FloatWritable> values, Context context
        ) throws IOException, InterruptedException {
            System.out.println("Reduce Method Invoked!");
            Iterator<FloatWritable> iterator = values.iterator();
            float sum = 0;
            int count = 0;
            while (iterator.hasNext()) {
                float tmp = iterator.next().get();
                sum += tmp;
                count++;
            }

            float averageScore = sum / count;
            context.write(key, new FloatWritable(averageScore));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: DefferentData <in> [<in>...] <out>");
            System.exit(2);
        }
        //Job job = new Job(conf, "practice.Average");
        Job job = Job.getInstance(conf, "practice.Average");
        job.setJarByClass(Average.class);
        job.setMapperClass(DataMapper.class);
        job.setCombinerClass(DataReducer.class);//加快效率
        job.setReducerClass(DataReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}