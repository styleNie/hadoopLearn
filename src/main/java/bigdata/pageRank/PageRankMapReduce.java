package bigdata.pageRank;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import bigdata.util.HadoopCfg;
import bigdata.util.HadoopUtil;

/**
 * PageRank
 *
 * @author wwhhf
 * http://blog.csdn.net/qq_17612199/article/details/51404656
 *
 */
public class PageRankMapReduce {

    private final static String JOB_NAME = "PageRank";
    private static String LINKS = "links";

    private static Map<String, Double> rand = new HashMap<String, Double>();

    private static final double a = 0.8;

    public static void initRand(String pathin, String filename) throws IOException {
        //  读取初始的各节点权重
        List<String> lines = HadoopUtil.lsAllFile(pathin, filename);
        for (String line : lines) {
            String terms[] = line.toString().split("\t");   // tab 分割
            rand.put(terms[0], Double.valueOf(terms[1]));
        }
    }

    private static class PageRankMapper extends Mapper<Object, Text, Text, DoubleWritable> {
                //  map函数的输入key默认为行号索引，只能为LongWritable或者 Object类型
        @Override
        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String filename = fileSplit.getPath().getName();
            //String filename = key.toString();
            if (filename.startsWith(LINKS)) {
                // 读取链接关系表，将权重平均分配到链接节点
                String dests[] = value.toString().split(" ");
                double e = rand.get(dests[0]);
                for (int i = 0, len = dests.length; i < len; i++) {
                    String dest = dests[i];
                    if (i == 0) {
                        context.write(new Text(dest), new DoubleWritable(0.0));
                    } else {
                        context.write(new Text(dest), new DoubleWritable(e / (len - 1)));
                    }
                }
            }
        }
    }

    private static class PageRankReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            Double sum = 0.0;
            for (DoubleWritable value : values) {
                sum += value.get();
            }
            double e = rand.get(key.toString());
            context.write(key, new DoubleWritable(a * sum + (1 - a) * e));
        }
    }

    public static void solve(String linksin, String pathin, String pathout)
            throws ClassNotFoundException, InterruptedException {
        try {
            Configuration cfg = HadoopCfg.getConfiguration();
            Job job = Job.getInstance(cfg);
            job.setJobName(JOB_NAME);
            job.setJarByClass(PageRankMapReduce.class);
            //job.setInputFormatClass(FileNameInputFormat.class);
            job.setInputFormatClass(TextInputFormat.class);  // 输入数据以 \t 分割
            job.setOutputFormatClass(TextOutputFormat.class);  // 输出数据以 \t 分割

            // mapper
            job.setMapperClass(PageRankMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(DoubleWritable.class);

            // reducer
            job.setReducerClass(PageRankReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(DoubleWritable.class);

            FileInputFormat.addInputPath(job, new Path(pathin));
            FileInputFormat.addInputPath(job, new Path(linksin));
            FileOutputFormat.setOutputPath(job, new Path(pathout));

            job.waitForCompletion(true);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws ClassNotFoundException,
            InterruptedException, IOException {
        String path = "input\\pagerank";
        String links_pathin = "input\\pagerank\\links";
        String filename = "part-r-";
        String tmp_pathin = path;
        for (int i = 1; i <= 2; i++) {
            initRand(tmp_pathin, filename);
            String tmp_pathout = path + i;
            System.out.println(links_pathin + " " + tmp_pathin + " " + tmp_pathout);
            solve(links_pathin, tmp_pathin, tmp_pathout);
            tmp_pathin = tmp_pathout;
        }
    }
}
