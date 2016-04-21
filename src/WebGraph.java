import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class WebGraph {

	
	public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable>{
		public void reduce (Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException{
			int sum = 0;
			
			while(values.hasNext()){
				sum += values.next().get();
			}
			output.collect(key, new IntWritable(sum));
		}
	}
	/**
	 * @param args
	 */
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
		//instance vars for Map class
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		
		 public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
		       String line = value.toString();
		       StringTokenizer tokenizer = new StringTokenizer(line);
		       while (tokenizer.hasMoreTokens()) {
					word.set(tokenizer.nextToken().toString() + " " + tokenizer.nextToken().toString());
					output.collect(word, one);
		       }
	     }
	}
	   


	public static void main(String[] args) {
		JobConf conf = new JobConf(WordCount.class);
		conf.setJobName("wordcount");
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		
		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);
		
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(conf, newPath(args[0]));
		FileInputFormat.setOutputPaths(conf, newPath(args[1]));
		
		JobClient.runJob(conf);

	}
	

}
