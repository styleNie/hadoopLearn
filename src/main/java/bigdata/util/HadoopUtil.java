package bigdata.util;

/**
 * Created by Administrator on 2016/12/26.
 */
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import bigdata.util.HadoopCfg;

/**
 * 处理hdfs文件系统
 *
 * @author wwhhf
 *
 */
public class HadoopUtil {

    /**
     * 创建目录
     *
     * @param dirPath
     */
    public static void createDir(String dirPath) {
        try {
            Configuration cfg = HadoopCfg.getConfiguration();
            FileSystem fs = FileSystem.get(cfg);
            fs.mkdirs(new Path(dirPath));
            fs.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void mv() throws IOException{
        Configuration cfg = HadoopCfg.getConfiguration();
        FileSystem fs = FileSystem.get(cfg);
    }

    /**
     * 删除目录或者文件
     *
     * @param dirPath
     * @param isThrow
     */
    public static void delete(String dirPath, Boolean isThrow) {
        try {
            Configuration cfg = HadoopCfg.getConfiguration();
            FileSystem fs = FileSystem.get(cfg);
            fs.delete(new Path(dirPath), isThrow == null ? false : isThrow);
            fs.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 创建文件
     *
     * @param localDir
     * @param destDir
     */
    public static void saveFile(String localDir, String destDir) {
        try {
            Configuration cfg = HadoopCfg.getConfiguration();
            FileSystem fs = FileSystem.get(cfg);
            Path localPath = new Path(localDir);
            Path destPath = new Path(destDir);
            fs.copyFromLocalFile(localPath, destPath);
            fs.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 创建文件
     *
     * @param destDir
     * @param file
     */
    public static void saveFile(String destDir, File file) {
        try {
            Configuration cfg = HadoopCfg.getConfiguration();
            FileSystem fs = FileSystem.get(cfg);
            Path localPath = new Path(file.getAbsolutePath());
            Path destPath = new Path(destDir);
            fs.copyFromLocalFile(localPath, destPath);
            fs.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 读取文件
     *
     * @param destDir
     * @param fileName
     * @param localDir
     * @return
     */
    public static File getFile(String destDir, String fileName, String localDir) {
        try {
            Configuration cfg = HadoopCfg.getConfiguration();
            FileSystem fs = FileSystem.get(cfg);
            Path path = new Path(destDir + File.separator + fileName);
            if (fs.exists(path)) {
                FSDataInputStream is = fs.open(path);
                FileStatus stat = fs.getFileStatus(path);
                byte[] buffer = new byte[Integer.parseInt(String.valueOf(stat.getLen()))];
                is.readFully(0, buffer);
                is.close();
                fs.close();
                //FileBytesUtil.bytesToFile(buffer, localDir, fileName);
                return new File(localDir + File.separator + fileName);
            } else {
                throw new Exception("the file is not found .");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 遍历目录文件
     *
     * @param path
     */
    public static FileStatus[] listFile(String path) {
        try {
            Configuration cfg = HadoopCfg.getConfiguration();
            FileSystem fs = FileSystem.get(cfg);
            Path destPath = new Path(path);
            FileStatus fileStatus[] = fs.listStatus(destPath);
            fs.close();
            return fileStatus;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 读取文件
     *
     * @param path
     * @param fileName
     * @return
     * @throws IOException
     */
    public static List<String> lslFile(String path, String fileName) throws IOException {
        Configuration cfg = HadoopCfg.getConfiguration();
        FileSystem fs = FileSystem.get(cfg);
        FSDataInputStream in = fs.open(new Path(path + File.separator + fileName));
        BufferedReader bi = new BufferedReader(new InputStreamReader(in));
        String line = null;
        List<String> res = new ArrayList<String>();
        while ((line = bi.readLine()) != null) {
            res.add(line);
        }
        bi.close();
        in.close();
        return res;
    }

    public static class pathFilter implements PathFilter {
        private final String regex;
        public pathFilter(String regex) {
            this.regex = regex;
        }

        public boolean accept(Path path) {
            if(path.toString().contains(this.regex)){
                return true;
            }else{
                return false;
            }
        }
    }

    public static List<String> lsAllFile(String parentPath, String fixFileName) throws IOException {
        Configuration cfg = HadoopCfg.getConfiguration();
        FileSystem fs = FileSystem.get(cfg);
        FileStatus[] hdfs = fs.globStatus(new Path(parentPath+"/*"),new pathFilter(fixFileName));
        Path[] listPath = FileUtil.stat2Paths(hdfs);
        List<String> res = new ArrayList<String>();
        for(Path p : listPath) {
            FSDataInputStream in = fs.open(p);
            BufferedReader bi = new BufferedReader(new InputStreamReader(in));
            String line = null;
            while ((line = bi.readLine()) != null) {
                res.add(line);
            }
            bi.close();
            in.close();
        }
        return res;
    }

    /**
     * 读取文件
     *
     * @param path
     * @param fileName
     * @return
     * @throws IOException
     */
    public static String lssFile(String path, String fileName) throws IOException {
        Configuration cfg = HadoopCfg.getConfiguration();
        FileSystem fs = FileSystem.get(cfg);
        FSDataInputStream in = fs.open(new Path(path + File.separator + fileName));
        BufferedReader bi = new BufferedReader(new InputStreamReader(in));
        StringBuffer sb = new StringBuffer();
        String line = "";
        while ((line = bi.readLine()) != null) {
            sb.append(line).append('\n');
        }
        bi.close();
        in.close();
        return sb.toString();
    }

    // hadoop主要是hdfs和mapreduce两大框架，hdfs用来存储文件，mapreduce用来处理文件进行计算。
    // 1.首先，对于hdfs，dn负责存储文件，以及文件的副本，而nn负责存储文件的元数据，
    // 例如文件的块信息，以及位置信息等，这些数据会保存在nn的内存中，当存在很多的小文件时，
    // 每个小文件nn存储的元数据都是一样的，所以N个小文件会占用nn大量的内存，增大nn的负担。
    // 2.而对于mapreduce来说，map的输入默认是一个输入文件对应一个map任务，
    // 所以如果不做特殊处理在使用mapreduce处理这些小文件时会一个小文件产生一个map。
    // 这样的话每个map只处理一个小文件，会造成很大的资源浪费，同时也会降低mapreduce的执行效率。

    public static void main(String[] args) throws IOException {
//         getFile("output", "HadoopApriori-master.zip", "D:\\");
//         listFile("output");
        getFile("D:\\hadoop","HadoopApriori-master.zip","D:\\test");
        listFile("D:\\hadoop");
    }
}
