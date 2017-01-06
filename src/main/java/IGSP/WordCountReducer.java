package IGSP;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>
{
	double minSupportNum;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException
	{
		minSupportNum = Double.parseDouble((context.getConfiguration().get("minSupportNum")));
	}
	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
	{
		int sum = 0;
		for (IntWritable value : values)
		{
			sum += value.get();
		}
		if (sum >= minSupportNum)
			context.write(key, new IntWritable(sum));
	}
}
