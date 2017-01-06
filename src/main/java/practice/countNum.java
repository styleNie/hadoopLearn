package practice;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.io.IOException;
import java.net.URI;

/**
 * Created by Administrator on 2016/12/28.
 * wordcount 并按key排序，输入数据每一行一个整数
 */
public class countNum extends Configured implements Tool {

    public static final Logger logger = Logger.getLogger(countNum.class);

    //map将输入中的value化成LongWritable类型，作为输出的key
    public static class Map extends Mapper<Object,Text,LongWritable,LongWritable> {
        private static LongWritable data = new LongWritable();
        private static LongWritable one = new LongWritable(1);
        //实现map函数
        public void map(Object key,Text value,Context context)
                throws IOException,InterruptedException {
            String line = value.toString();
            if(line !="") {
                data.set(Long.parseLong(line));
                context.write(data, one);
            }
        }
    }

    public static class MyReduce extends Reducer<LongWritable,LongWritable,LongWritable,LongWritable> {
        private LongWritable result = new LongWritable();
        public void reduce(LongWritable key,Iterable<LongWritable> values,Context context)
                throws IOException,InterruptedException{
            long sum = 0;
            for(LongWritable val:values){
                sum += val.get();
            }
            result.set(sum);
            context.write(key,result);
        }
    }

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        conf.set("mapred.job.tracker", "localhost:9001");
        if (args.length != 2) {
            System.err.println("Usage: Data Sort <in> <out>");
            System.exit(2);
        }
        Path in = new Path(args[0]);
        Path out= new Path(args[1]);
        Job job = new Job(conf, "Data Sort");
        job.setJarByClass(countNum.class);
        out.getFileSystem(conf).delete(out, true);
        job.setMapperClass(Map.class);
        job.setReducerClass(MyReduce.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(job, in);
        FileOutputFormat.setOutputPath(job, out);
        job.setNumReduceTasks(1);

//      RandomSampler第一个参数表示key会被选中的概率，第二个参数是一个选取samples数，第三个参数是最大读取input splits数
        MyInputSampler.Sampler<Text, Text> sampler = new MyInputSampler.RandomSampler<Text, Text>(0.1,100,10);
        MyInputSampler.writePartitionFile(job, sampler);
        job.setPartitionerClass(TotalOrderPartitioner.class);
        String partitionFile = TotalOrderPartitioner.getPartitionFile(getConf());
        URI partitionUri= new URI(partitionFile+"#"+TotalOrderPartitioner.DEFAULT_PATH);
        job.addCacheArchive(partitionUri);

        return job.waitForCompletion(true)?0:-1;
    }

    public static void main(String[] args) throws Exception{
        logger.info("countNum starting");
        long start = System.currentTimeMillis();
        ToolRunner.run(new Configuration(), new countNum(),args);
        long end = System.currentTimeMillis();
        System.out.println("time cost : "+(end-start)/1000);
        logger.info("countNum end");
    }
}