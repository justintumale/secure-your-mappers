import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WebGraphPart3Reducer_
  extends Reducer<Text, Text, Text, Text> {
  
  @Override
  public void reduce(Text key, Iterable<Text> values,
      Context context)
      throws IOException, InterruptedException {

      HashSet<Text> hashset = new HashSet<Text>();
  
      for (Text val : values){
    	  	Text col = new Text(val);
    	  	if (!hashset.contains(col)){
    	  		hashset.add(col);
    	  	}
      }
      Iterator<Text> iter = hashset.iterator();
      StringBuilder s1 = new StringBuilder();
      s1.append("[");

      while (iter.hasNext()){
    	  s1.append(iter.next());
    	  if(iter.hasNext()){
    		  s1.append(" ");
    	  }
      }
     
      s1.append("]");
      context.write(key, new Text(s1.toString()));   
  }

}
