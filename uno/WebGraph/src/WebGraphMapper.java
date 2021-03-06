import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WebGraphMapper 
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
      //this.val = new IntWritable(Integer.parseInt(itr.nextToken().toString()));
      word2.set(itr.nextToken());
      // the following check is that the word starts with an alphabet. 
      //if(Character.isAlphabetic((word.toString().charAt(0)))){
    	  //context.write(word, word2);
    	  context.write(word, word2);
    	  
      //}
    }
  }

}