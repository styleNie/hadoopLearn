package IGSP;

import java.io.*;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HDFSUtils
{
	static Configuration conf;
	static FileSystem    hdfs;
	static
	{
		conf = new Configuration();
		conf.set("fs.default.name","hdfs://master:9000");
		try
		{
			hdfs = FileSystem.get(conf);
		} catch (IOException e)
		{
			e.printStackTrace();
		}
	}

	public static void upLoad(String s,String d)
	{
		Path src = new Path(s);
		Path dst = new Path(d);
		try
		{
			hdfs.copyFromLocalFile(src, dst);
		} catch (IOException e)
		{
			e.printStackTrace();
			System.out.println("upload error");
		}
	}

	public static void mkdir(String dir)
	{
		try
		{
			if(dir.charAt(0)!='/')
				dir = "/"+dir;
			hdfs.mkdirs(new Path(dir));
		} catch (IllegalArgumentException e)
		{
			e.printStackTrace();
		} catch (IOException e)
		{
			e.printStackTrace();
		}
	}

	public static void deleteDir(String dir)
	{
		try
		{
			if(dir.equals("/"))
				System.out.println("不允许删除全部文件");
			else if(dir.charAt(0)!='/')
				dir = "/"+dir;
			hdfs.delete(new Path(dir),true);
		} catch (IllegalArgumentException e)
		{
			// TODO 自动生成的 catch 块
			e.printStackTrace();
		} catch (IOException e)
		{
			// TODO 自动生成的 catch 块
			e.printStackTrace();
		}
	}

	public static void copyFile(String s,String d)
	{
		Path src = new Path(s);
		Path dst = new Path(d);
		try
		{
			InputStreamReader iReader = new InputStreamReader(new FSDataInputStream(hdfs.open(src)));
			OutputStreamWriter oWriter = new OutputStreamWriter(new FSDataOutputStream(hdfs.create(dst)));
			IOUtils.copy(iReader, oWriter);
			iReader.close();
			oWriter.close(); 
		} catch (IOException e)
		{
			e.printStackTrace();
			System.out.println("复制出错");
		}
	}

	public static void appendLocal(String s)
	{
		Path src = new Path(s);
		File file = new File(Constants.fileDictory+"/result");
		try
		{
			InputStreamReader iReader = new InputStreamReader(new FSDataInputStream(hdfs.open(src)));
			OutputStreamWriter oWriter = new OutputStreamWriter(new FileOutputStream(file,true));
			oWriter.write("\r\n");
			IOUtils.copy(iReader, oWriter);
			iReader.close();
			oWriter.close(); 
		} catch (IOException e)
		{
			e.printStackTrace();
		}
	}

	public static boolean fileExists(String s) throws IOException
	{
		Path path = new Path(s);
		if(!hdfs.exists(path)||hdfs.open(path).read() == -1)
			return false;
		return true;
	}
}
