package practice;
/**
 * Created by Administrator on 2016/12/26.
 */
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

public class  FilesList
{
    public static class Filter implements PathFilter {
        public Filter() {
            super();
        }
//      过滤目录不包含指定文件名的文件
        public boolean accept(Path path) {
            if(path.toString().contains("part-r-")){
                return true;
            }else{
                return false;
            }
        }
    }

    public static void main(String[] args) throws Exception
    {
        if(args.length != 1){
            System.out.println("Usage : practice.FilesList <target>");
            System.exit(1);
        }
        Configuration conf = new Configuration();
        FileSystem hdfs = FileSystem.get(conf); // URI.create(args[0]),
        FileStatus[] fs = hdfs.globStatus(new Path("apriori_out1"),new Filter());
        //FileStatus[] fs = hdfs.listStatus(new Path(args[0]));
        Path[] listPath = FileUtil.stat2Paths(fs);
        for(Path p : listPath)
            System.out.println(p);
    }
}
