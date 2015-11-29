package hadoopQ10;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import abstracthadoop.CustomReducer;
public class Reducer extends CustomReducer <Text, DoubleWritable> {

	@Override
	public void reduce(Text key, Iterator<DoubleWritable> values, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
		  /**
		   * Durchschnitt der Werte bilden
		   * und dem jeweiligen Key zuordnen
		   * */
		
		  double sum  = 0;
		  int count = 0;
		  while (values.hasNext()) {
			  sum += values.next().get();
			  count++;
	      }
		  
	      output.collect(key , new DoubleWritable(sum/count));
	}

	
}

