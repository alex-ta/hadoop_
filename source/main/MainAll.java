package main;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

import abstracthadoop.CustomMapper;
import abstracthadoop.CustomReducer;



public class MainAll {

	public static void main(String[] args) {
		
		/** Einlesen der Commandline Arguments */
	    String output = args[1]; // 2 args
	    String input = args[0]; // 1 args
		
	    
	    /** Durchlaufen der Jobs mit jeweiliger Configuration */
	    changeConfig(hadoopQ1.Mapper.class, hadoopQ1.Reducer.class, input, output+"/Q1",Text.class,DoubleWritable.class);
	    changeConfig( hadoopQ2.Mapper.class, hadoopQ2.Reducer.class,input, output+"/Q2",Text.class,LongWritable.class);
	    changeConfig( hadoopQ3.Mapper.class, hadoopQ3.Reducer.class,input, output+"/Q3",Text.class,DoubleWritable.class);
	    changeConfig( hadoopQ4.Mapper.class, hadoopQ4.Reducer.class,input, output+"/Q4",IntWritable.class,DoubleWritable.class);
	    changeConfig( hadoopQ9.Mapper.class, hadoopQ9.Reducer.class,input, output+"/Q9",DoubleWritable.class,DoubleWritable.class);
	    changeConfig( hadoopQ10.Mapper.class, hadoopQ10.Reducer.class,input, output+"/Q10",Text.class,DoubleWritable.class);	    
	  }

	public static void changeConfig(Class<? extends CustomMapper<?,?>> mapper, Class<? extends CustomReducer<?,?>> reducer, String input,String output, Class<?> key, Class<?> value){
	    /** Erstellen der Configuration */
		JobConf conf = new JobConf(MainAll.class);
		conf.setJobName("Running Here");		    
		conf.setOutputKeyClass(key);
	    conf.setOutputValueClass(value);
	    /** Mapper und Reducer ohne Combiner */
	    conf.setMapperClass(mapper);
	    conf.setReducerClass(reducer);
	    conf.setInputFormat(TextInputFormat.class);
	    conf.setOutputFormat(TextOutputFormat.class);
	    FileInputFormat.setInputPaths(conf, new Path(input));
	    FileOutputFormat.setOutputPath(conf, new Path(output));
	    try {
			JobClient.runJob(conf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    
	}
	
}

