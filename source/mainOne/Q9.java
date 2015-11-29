package mainOne;
import hadoopQ9.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;


public class Q9 {

	public static void main(String[] args) throws Exception {
		
	    JobConf conf = new JobConf(new Configuration());
	    conf.setJobName("Running Here");
	    
	    conf.setOutputKeyClass(DoubleWritable.class);
	    conf.setOutputValueClass(DoubleWritable.class);
	    
	    conf.setMapperClass(Mapper.class);
	    conf.setReducerClass(Reducer.class);
	    //conf.setCombinerClass(Combiner.class);

	    conf.setInputFormat(TextInputFormat.class);
	    conf.setOutputFormat(TextOutputFormat.class);
	    

	    FileInputFormat.setInputPaths(conf, new Path(args[0]));
	    FileOutputFormat.setOutputPath(conf, new Path(args[1]));

	    JobClient.runJob(conf);
	  }
	
}
