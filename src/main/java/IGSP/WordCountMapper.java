package IGSP;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>
{
	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		StringTokenizer stringTokenizer = new StringTokenizer(value.toString());
		while (stringTokenizer.hasMoreTokens())
		{
			word.set("["+stringTokenizer.nextToken()+"]");
			context.write(word, one);
		}
	}
}
