package IGSP;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
//import org.apache.jasper.tagplugins.jstl.core.Out;

public class IGSPDriver extends Configured implements Tool
{
	private static int parts = 1; // 分块数
	private static int k = 0;

	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 4)
		{
			System.out.println("请输入正确的参数！用法：IGSP <输入数据> <最小支持度> <序列长度K> <分块数>");
			return;
		}
		Constants.filePath = args[0];
		Constants.fileDictory = new Path(args[0]).getParent().toString();
		Constants.minSupport = Double.parseDouble(args[1]);
		k = Integer.parseInt(args[2]);
		parts=Integer.parseInt(args[3]);
		
		carveupdatabase();
		uploadFiles();
		System.out.println("**********上传完毕**********");
		System.out.println("**********任务1开始**********");
		Configuration conf1 = new Configuration();
		conf1.set("minSupportNum", String.valueOf(Constants.minSupportNum));
		Job job1 = new Job(conf1, "First Job");
		job1.setJarByClass(IGSPDriver.class);
		job1.setMapperClass(WordCountMapper.class);
		job1.setReducerClass(WordCountReducer.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job1, new Path(Constants.rootPath, Constants.inputPath));
		FileOutputFormat.setOutputPath(job1, new Path(Constants.rootPath, Constants.outputPath[0]));
		job1.waitForCompletion(true);
		System.out.println("**********任务1完成**********");
		for (int i = 0; i < k; i++)
		{
			if (!HDFSUtils.fileExists(Constants.outputPath[0] + "/part-r-00000"))
			{
				System.out.println("over");
				break;
			}
				
			ToolRunner.run(new IGSPDriver(), args);
		}
	}

	//@Override // 执行MapReduce
	public int run(String[] args) throws Exception
	{
		HDFSUtils.copyFile(Constants.outputPath[0] + "/part-r-00000", "/IGSP/temp/lk");
		HDFSUtils.appendLocal(Constants.outputPath[0] + "/part-r-00000");
		HDFSUtils.deleteDir(Constants.outputPath[0]);
		Configuration conf = new Configuration();
		conf.set("minSupportNum", String.valueOf(Constants.minSupportNum));
		conf.set("inputPath", String.format("%s/IGSP/temp/lk", Constants.rootPath));
		Job job = new Job(conf, "general job");
		job.setJarByClass(IGSPDriver.class);
		job.setMapperClass(IGSPMapper.class);
		job.setReducerClass(IGSPReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(Constants.rootPath, Constants.inputsPath));
		FileOutputFormat.setOutputPath(job, new Path(Constants.rootPath, Constants.outputPath[0]));
		job.waitForCompletion(true);
		return 1;
	}
	// 将输入数据平分为parts块
	public static void carveupdatabase()
	{
		try
		{
			String line = "";
			int sum = 0;
			BufferedReader bReader = new BufferedReader(new FileReader(Constants.filePath));
			ArrayList<BufferedWriter> bufferedWriters = new ArrayList<BufferedWriter>();
			for (int i = 1; i <= parts; i++)
			{
				bufferedWriters.add(new BufferedWriter(new FileWriter(Constants.fileDictory + "/part" + i)));
			}
			while ((line = bReader.readLine()) != null)
			{
				bufferedWriters.get(sum % parts).write(line);
				bufferedWriters.get(sum % parts).newLine();
				bufferedWriters.get(sum % parts).flush();
				sum++;
			}
			bReader.close();
			Constants.minSupportNum = sum * Constants.minSupport;
		} catch (IOException e)
		{
			e.printStackTrace();
			System.out.println("File not exists!");
		}
	}
	// 上传到HDFS
	public static void uploadFiles()
	{
		HDFSUtils.deleteDir("/IGSP");
		HDFSUtils.mkdir(Constants.inputPath);
		HDFSUtils.upLoad(Constants.filePath, Constants.inputPath);
		HDFSUtils.mkdir(Constants.inputsPath);
		for (int i = 1; i <= parts; i++)
		{
			HDFSUtils.upLoad(Constants.fileDictory + "/part" + i, Constants.inputsPath + "/part" + i);
		}
		// HDFSUtils.mkdir(Constants.outputPath);
		// for (int i = 1; i <= parts; i++)
		// {
		// HDFSUtils.mkdir(Constants.outputPath + "/output" + i);
		// }
	}
}
