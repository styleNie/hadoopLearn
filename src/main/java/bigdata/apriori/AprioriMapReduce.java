package bigdata.apriori;

/**
 * Created by Administrator on 2016/12/25.
 * http://blog.csdn.net/qq_17612199/article/details/51473580
 */
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import bigdata.util.HadoopCfg;
import bigdata.util.HadoopUtil;

public class AprioriMapReduce {

    public static final int MIN_SUPPORT = 2;

    private static final String JOB_NAME = "Apriori";

    public static class AprioriMapper extends Mapper<LongWritable, Text, Text, LongWritable>{

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String terms[] = value.toString().split(",");
            for (String term : terms) {
                context.write(new Text(term), new LongWritable(1));
            }
        }
    }

    public static class AprioriReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {
            Long sum = 0L;
            for (LongWritable value : values) {
                sum += value.get();
            }
            if (sum >= MIN_SUPPORT) {
                context.write(key, new LongWritable(sum));
            } else {
                System.out.println("key: " + key.toString() + ", sum: " + sum);
            }
        }
    }

    public static class AprioriKMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

        private List<AprioriNode> preWords = new ArrayList<AprioriNode>();
        private List<AprioriNode> preNodes = new ArrayList<AprioriNode>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // 读取 K 频繁项集
            super.setup(context);
            Configuration cfg = context.getConfiguration();
            String pathin = cfg.get("pathin");
            List<String> lines = HadoopUtil.lsAllFile(pathin, "part-r-");
            for (String line : lines) {
                String terms[] = line.split("\t");
                String words[] = terms[0].split(",");
                preNodes.add(new AprioriNode(terms[0]));
                if (words.length == 1) {
                    preWords.add(new AprioriNode(words[0]));
                } else if (words.length > 1) {
                    for (String word : words) {
                        preWords.add(new AprioriNode(word));
                    }
                }
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            // 由 K 项集得到候选 K+1 项集
            String terms[] = value.toString().split("\t");
            AprioriNode node = new AprioriNode(terms[0]);
            for (AprioriNode preWord : preWords) {
                AprioriNode newObj = node.copy();
                if (newObj.add(preWord.getFront()) == true) {  // 拼接后的 K+1 项集元素不重复
                    List<AprioriNode> subNodes = AprioriUtil.genSubNodes(newObj);  //  由K+1项集拆分得到K项集
                    boolean flag = true;
                    for (AprioriNode subNode : subNodes) {
                        if (preNodes.contains(subNode) == false) {
                            // 判断拆分得到的K项集是否包含在频繁K项集中，如果不是，则候选的K+1项集不可能为频繁项集
                            flag = false;
                            break;
                        }
                    }
                    if (flag == true) {
                        context.write(new Text(newObj.toString()), NullWritable.get());
                    }
                }
            }
        }
    }

    public static class AprioriKReducer extends Reducer<Text, NullWritable, Text, LongWritable> {

        private List<AprioriNode> clist = new ArrayList<AprioriNode>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // 扫描原始数据文件
            super.setup(context);
            Configuration cfg = context.getConfiguration();
            String pathin = cfg.get("datain");
            List<String> lines = HadoopUtil.lslFile(pathin, "data");
            for (String line : lines) {
                clist.add(new AprioriNode(line));
            }
        }

        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context)
                throws IOException, InterruptedException {
            AprioriNode node = new AprioriNode(key.toString());
            Long sum = 0L;
            for (AprioriNode cnode : clist) {
                // data
                if (cnode.isContained(node) == true) {
                    sum = sum + 1L;
                }
            }
            if (sum >= MIN_SUPPORT) {
                context.write(key, new LongWritable(sum));
            }
        }
    }

    public static void c1(String pathin, String pathout) throws ClassNotFoundException, InterruptedException {
        try {
            Configuration cfg = HadoopCfg.getConfiguration();
            Job job = Job.getInstance(cfg);
            job.setJobName(JOB_NAME);
            job.setJarByClass(AprioriMapReduce.class);

            // mapper
            job.setMapperClass(AprioriMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(LongWritable.class);

            // reducer
            job.setReducerClass(AprioriReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(LongWritable.class);

            job.setNumReduceTasks(2);
            FileInputFormat.addInputPath(job, new Path(pathin));
            FileOutputFormat.setOutputPath(job, new Path(pathout));

            job.waitForCompletion(true);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void f1(String datain, String pathin) throws ClassNotFoundException, InterruptedException {
        try {
            Configuration cfg = HadoopCfg.getConfiguration();
            int k = 2;
            for (int i = 1; i <= k; i++) {
                cfg.set("datain", datain);
                cfg.set("pathin", pathin + i);
                Job job = Job.getInstance(cfg);
                job.setJobName(JOB_NAME);
                job.setJarByClass(AprioriMapReduce.class);

                // mapper
                job.setMapperClass(AprioriKMapper.class);
                job.setMapOutputKeyClass(Text.class);
                job.setMapOutputValueClass(NullWritable.class);

                // reducer
                job.setReducerClass(AprioriKReducer.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(LongWritable.class);
                job.setNumReduceTasks(2);

                FileInputFormat.addInputPath(job, new Path(pathin + i));
                FileOutputFormat.setOutputPath(job, new Path(pathin + (i + 1)));

                job.waitForCompletion(true);

            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws ClassNotFoundException, InterruptedException {
        c1("input\\apriori", "apriori_out1");
        f1("input\\apriori", "apriori_out");
    }
}
