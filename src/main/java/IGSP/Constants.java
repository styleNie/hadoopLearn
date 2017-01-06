package IGSP;

public class Constants
{

	/**最小支持度计数 */
	public static double minSupportNum;
	/**最小支持度 */
	public static double minSupport;
	/**本机输入文件的路径 */
	public static String filePath;
	/**本机运行文件所在的文件夹 */
	public static String fileDictory;
	/**HDFS输入文件路径	*/
	public static final String inputPath="/IGSP/input";
	/**HDFS分割后输入文件路径	*/
	public static final String inputsPath="/IGSP/inputs";
	/**结果输出路径 */
	public static final String[] outputPath=new String[]{"/IGSP/output1","/IGSP/output2","/IGSP/output3"};
	/**HDFS路径 */
	public static final String rootPath="hdfs://master:9000";
}
