/**
  * Created by Administrator on 2016/12/22.
  */

import java.io.IOException
import java.util._

import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf._
import org.apache.hadoop.io._
import org.apache.hadoop.mapred._
import org.apache.hadoop.util._

class Map extends MapReduceBase with Mapper[LongWritable, Text, Text, IntWritable] {
  private val one = new IntWritable(1)
  private val word = new Text()

  def map(key: LongWritable, value: Text, output: OutputCollector[Text, IntWritable], reporter: Reporter) {
    val line = value.toString
    val tokenizer = new StringTokenizer(line)
    while(tokenizer.hasMoreTokens) {
      word.set(tokenizer.nextToken)
      output.collect(word, one)
    }
  }
}

class Reduce extends MapReduceBase with Reducer[Text, IntWritable, Text, IntWritable] {
  def reduce(key: Text, values: Iterator[IntWritable], output: OutputCollector[Text, IntWritable], reporter: Reporter) {
    output.collect(key, new IntWritable(count(0, values)))

    def count(sum: Int, vs: Iterator[IntWritable]): Int =
      if(vs.hasNext)
        count(sum + vs.next.get, vs)
      else
        sum
  }
}

object scalaHadoopWordcount {
  def main(args: Array[String]) {
    val conf = new JobConf(this.getClass)
    conf.setJobName("WordCount")
    conf.setOutputKeyClass(classOf[Text])
    conf.setOutputValueClass(classOf[IntWritable])

    conf.setMapperClass(classOf[Map])
    conf.setCombinerClass(classOf[Reduce])
    conf.setReducerClass(classOf[Reduce])
    conf.setInputFormat(classOf[TextInputFormat])
    conf.setOutputFormat(classOf[TextOutputFormat[Text, IntWritable]])

    FileInputFormat.setInputPaths(conf, new Path(args(0)))
    FileOutputFormat.setOutputPath(conf, new Path(args(1)))

    JobClient.runJob(conf)
  }
}
