package bigdata.matrixMultiply;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import bigdata.util.HadoopCfg;
import bigdata.util.NodeWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import bigdata.util.NodeWritable.Node;

/**
 * 大矩阵相乘
 *
 * @author wwhhf
 * http://blog.csdn.net/qq_17612199/article/details/51345240
 *
 */
public class MatrixMapReduce {

    public static class MatrixMapper extends Mapper<LongWritable, Text, Text, NodeWritable> {

        private int M = 0;
        private int N = 0;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration config = context.getConfiguration();
            M = config.getInt("M", 0);
            N = config.getInt("N", 0);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String fileName = fileSplit.getPath().getName();

            String terms[] = value.toString().split(" ");
            String xy[] = terms[0].split(",");
            int x = Integer.valueOf(xy[0]);
            int y = Integer.valueOf(xy[1]);
            long val = Long.valueOf(terms[1]);

            // 矩阵M*N
            if (fileName.startsWith("M")) {
                // 矩阵M
                for (int i = 1; i <= N; i++) {
                    context.write(new Text(x + "," + i), new NodeWritable(NodeWritable.M, x, y, val));
                }
            } else {
                // 矩阵N
                for (int i = 1; i <= M; i++) {
                    context.write(new Text(i + "," + y), new NodeWritable(NodeWritable.N, x, y, val));
                }
            }
        }
    }

    public static class MatrixReducer extends Reducer<Text, NodeWritable, Text, LongWritable> {

        @Override
        protected void reduce(Text key, Iterable<NodeWritable> values, Context context)
                throws IOException, InterruptedException {
            List<Node> MMatrix = new ArrayList<Node>();
            List<Node> NMatrix = new ArrayList<Node>();
            for (NodeWritable value : values) {
                if (value.getFlag() == NodeWritable.M) {
                    // M
                    MMatrix.add(value.getNode());
                } else {
                    // N
                    NMatrix.add(value.getNode());
                }
            }
            Collections.sort(MMatrix);   // 按在矩阵中的相对位置从上到下，从左到右排序，便于后续按对应位置做乘法
            Collections.sort(NMatrix);
            if (NMatrix.size() == MMatrix.size()) {
                long sum = 0L;
//                for (Node a : MMatrix) {
//                    for (Node b : NMatrix) {
//                        sum = sum + (a.getVal() * b.getVal());
//                    }
//                }
                for(int i=0;i<MMatrix.size();i++){
                    sum = sum + (MMatrix.get(i).getVal() * NMatrix.get(i).getVal());
                }
                context.write(key, new LongWritable(sum));
            }
        }
    }

    public static void main(String[] args) {
        try {
            Configuration cfg = HadoopCfg.getConfiguration();
            cfg.setInt("M", 4);
            cfg.setInt("K", 5);
            cfg.setInt("N", 6);

            Job job = Job.getInstance(cfg);
            job.setJobName("Matrix");
            job.setJarByClass(MatrixMapReduce.class);

            // mapper
            job.setMapperClass(MatrixMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(NodeWritable.class);

            // reducer
            job.setReducerClass(MatrixReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(LongWritable.class);

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            System.exit(job.waitForCompletion(true) ? 0 : 1);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
