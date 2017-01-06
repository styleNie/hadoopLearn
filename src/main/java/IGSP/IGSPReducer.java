package IGSP;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class IGSPReducer extends Reducer<Text, IntWritable, Text, IntWritable>
{
	double minSupportNum;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException
	{
		minSupportNum = Double.parseDouble(context.getConfiguration().get("minSupportNum"));
	}
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
	{
		int sum=0;
		for(IntWritable i:values)
		{
			sum+=i.get();
		}
		if(sum>=minSupportNum)
		{
			context.write(key, new IntWritable(sum));
		}
	}
}
