import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WebGraphReducer
  extends Reducer<Text, Text, Text, Text> {
  
  @Override
  public void reduce(Text key, Iterable<Text> values,
      Context context)
      throws IOException, InterruptedException {

      Text col = new Text();
      int sum = 0;
      Text lastVal = new Text("proxy");
      for (Text val : values) {
    	  if (!val.equals(lastVal)){
    		  col.set(val);
    		  context.write(key, col);
    		  lastVal = col;
    	 }
        
      }

  }

}

