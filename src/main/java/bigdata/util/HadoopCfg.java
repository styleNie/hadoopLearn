package bigdata.util;

/**
 * Created by Administrator on 2016/12/26.
 */
import org.apache.hadoop.conf.Configuration;

public class HadoopCfg {

    public static Configuration cfg = null;

    public static synchronized Configuration getConfiguration() {
        if (cfg == null) {
            cfg = new Configuration();
//            cfg.addResource(HadoopCfg.class.getResource("core-site.xml"));
//            cfg.addResource(HadoopCfg.class.getResource("hdfs-site.xml"));
//            cfg.addResource(HadoopCfg.class.getResource("yarn-site.xml"));
        }
        return cfg;
    }

}
