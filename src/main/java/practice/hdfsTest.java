package practice; /**
 * Created by Administrator on 2016/12/23.
 */
import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

class hdfsTest {
    public static void main(String args[]) throws IOException {
        download();
    }
    // 下载文件
    public static void download() throws IOException
    {
        //以下两行用来指明登陆hadoop的用户和你本地的hadoop-2.6.0所存的目录。
        System.setProperty("HADOOP_USER_NAME", "hadoop上的用户名");
        System.setProperty("hadoop.home.dir", "E:\\hadoop\\hadoop-2.6.0");

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path src = new Path("D:\\");  // 目标文件
        Path path = new Path("hdfs://10.0.0.51:8020/tmp/bbkt.log"); //源文件
        fs.copyToLocalFile(path,src);
    }
}
