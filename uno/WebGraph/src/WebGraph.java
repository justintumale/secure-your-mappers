import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WebGraph {

  public static void main(String[] args) throws Exception {
	
    if (args.length != 2) {
      System.err.println("Usage: WebGraph <input path> <output path>");
      System.exit(-1);
    }

    Job job = new Job();
    job.setJarByClass(WebGraph.class);
    job.setJobName("Web Graph");
   

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    job.setMapperClass(WebGraphMapper.class);
    job.setReducerClass(WebGraphReducer.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
   
    /*
	JobConf conf = new JobConf(WebGraph.class);
	conf.setJobName("web graph");
	
	conf.setOutputKeyClass(Text.class);
	conf.setOutputValueClass(IntWritable.class);
	
	conf.setMapperClass((Class<? extends Mapper>) WebGraphMapper.class);
	conf.setCombinerClass((Class<? extends Reducer>) WebGraphReducer.class);
	conf.setReducerClass((Class<? extends Reducer>) WebGraphReducer.class);
	
	conf.setInputFormat(TextInputFormat.class);
	conf.setOutputFormat(TextOutputFormat.class);
	
	FileInputFormat.setInputPaths(conf, new Path(args[0]));
	FileOutputFormat.setOutputPath(conf, new Path(args[1]));
	
	JobClient.runJob(conf);
	*/

   }
}
