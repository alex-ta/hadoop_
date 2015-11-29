package hadoopQ1;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import abstracthadoop.CustomReducer;
public class Reducer extends CustomReducer <Text,DoubleWritable> {

	@Override
	public void reduce(Text key, Iterator<DoubleWritable> values, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
		  double sum  = 0;
		  int count = 0;
		  /**
		   * Durchschnitt der Werte bilden
		   * und dem jeweiligen Key zuordnen
		   * */
	      while (values.hasNext()) {
	    	  double current = values.next().get();
	    	  sum += current;
	    	  count++;
	      }
	      output.collect(new Text(key.toString()), new DoubleWritable(sum/count));
	}

	
}

