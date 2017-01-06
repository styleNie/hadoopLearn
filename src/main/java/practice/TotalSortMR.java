package practice; /**
 * Created by Administrator on 2016/12/23.
 * http://www.linuxidc.com/Linux/2013-07/87102.htm
 * 全局排序，未完
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TotalSortMR extends Configured implements Tool{

    public static class MyPartitioner<K1, V1> extends Partitioner<K1, V1> {

        @Override
        public int getPartition(K1 key, V1 value, int numPartitions) {
            String tmpValue = value.toString();
            return (Integer.valueOf(key.toString()) & Integer.MAX_VALUE) % numPartitions;
        }
    }

    public  int run(String[] args) throws Exception {
        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        //Path partitionFile = new Path(args[2]);
        int reduceNumber = Integer.parseInt(args[3]);

        // RandomSampler第一个参数表示key会被选中的概率，第二个参数是一个选取samples数，第三个参数是最大读取input splits数
        //RandomSampler<Text, Text> sampler = new InputSampler.RandomSampler<Text, Text>(0.1, 3, 10);

        Configuration conf = new Configuration();
        // 设置partition file全路径到conf
        //TotalOrderPartitioner.setPartitionFile(conf, partitionFile);

        Job job = new Job(conf);
        job.setJobName("Total-Sort");
        job.setJarByClass(TotalSortMR.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setNumReduceTasks(reduceNumber);

        // partitioner class设置成TotalOrderPartitioner
        job.setPartitionerClass(TotalOrderPartitioner.class);

        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        outputPath.getFileSystem(conf).delete(outputPath, true);

//        String partitionFile = TotalOrderPartitioner.getPartitionFile(getConf());
//        URI partitionUri= new URI(partitionFile+"#"+TotalOrderPartitioner.DEFAULT_PATH);
//        job.addCacheArchive(partitionUri);

        // 写partition file到mapreduce.totalorderpartitioner.path
//        InputSampler.writePartitionFile(job, sampler);

        return job.waitForCompletion(true)? 0 : 1;

    }

    public static void main(String[] args) throws Exception{
        ToolRunner.run(new Configuration(), new TotalSortMR(),args);
    }
}
