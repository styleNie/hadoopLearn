package IGSP;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class IGSPMapper extends Mapper<LongWritable, Text, Text, IntWritable>
{
	String inputPath = "";
	SeqDB seqdb = new SeqDB();
	SeqDB lk = new SeqDB();

	@Override
	protected void setup(Context context) throws IOException, InterruptedException
	{
		try
		{
			inputPath = context.getConfiguration().get("inputPath");
			FileSystem fs = new Path(inputPath).getFileSystem(new Configuration());
			BufferedReader bReader = new BufferedReader(new InputStreamReader(new FSDataInputStream(fs.open(new Path(inputPath)))));
			String line = "";
			while ((line = bReader.readLine()) != null)
			{
				Sequence temp = new Sequence();
				String[] seq = line.split("\t");
				temp.setSupport(Integer.parseInt(seq[1]));
				String[] item = seq[0].substring(1, seq[0].length() - 1).split(",");
				for (int i = 0; i < item.length; i++)
				{
					temp.addElement(new Element(item[i].trim()));
				}
				lk.seqs.add(temp);
			}
			bReader.close();
		} catch (Exception e)
		{
			e.printStackTrace();
		}
	}
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		Sequence s = new Sequence();
		for (String el : value.toString().split(" "))
			if (!el.trim().isEmpty())
				s.addElement(new Element(el));
		seqdb.seqs.add(s);
	}
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException
	{
		lk.seqs=IGSPUtils.genCandidate(lk.seqs);
		for(int i=0;i<seqdb.seqs.size();i++)
		{
			for(int j=0;j<lk.seqs.size();j++)
			{
				if(lk.seqs.get(j).isSubsequenceOf(seqdb.seqs.get(i)))
				{
					lk.seqs.get(j).increaseSupport();
				}
			}
		}
		for(int i=0;i<lk.seqs.size();i++)
		{
			context.write(new Text(lk.seqs.get(i).toString()), new IntWritable(lk.seqs.get(i).getSupport()));
		}
	}
}
