import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WebGraphPart3Reducer
  extends Reducer<Text, Text, Text, Text> {
  
  @Override
  public void reduce(Text key, Iterable<Text> values,
      Context context)
      throws IOException, InterruptedException {

      Text col = new Text();

      int indegree = 0;
      int outdegree = 0;
      Text lastVal = new Text("proxy");
      for (Text val : values) {
    		col.set(val);
    		if (val.equals(new Text("1 0"))){
    			indegree++;
    		}
    		else{
    			outdegree++;
    		}
      }
      String iStr = String.valueOf(indegree);
      String oStr = String.valueOf(outdegree);
      context.write(key, new Text(iStr + " " + oStr));   
  }

}
