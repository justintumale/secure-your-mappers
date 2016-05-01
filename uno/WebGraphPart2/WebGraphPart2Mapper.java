import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WebGraphPart2Mapper
			//input key, value   //output key value
  extends Mapper<LongWritable, Text, Text, Text> {

  //private final static IntWritable one = new IntWritable(1);
  private IntWritable val;
	private Text word = new Text();
	private Text word2 = new Text();

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    String line = value.toString();
    StringTokenizer itr = new StringTokenizer(line);
    System.out.println(line);
    while (itr.hasMoreTokens()) {
   //just added the below line to convert everything to lower case 
      word.set(itr.nextToken());
      context.write(word, new Text("1 0"));
      word2.set(itr.nextToken());
      context.write(word2, new Text("0 1"));

    }
  }

}

    
