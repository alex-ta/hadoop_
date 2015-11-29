package hadoopQ1;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

import abstracthadoop.CustomMapper;

public class Mapper extends CustomMapper<Text, DoubleWritable> {
   
	private static int line = 0;
	
    public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
      /**
       * Ignorieren der ersten Eingabe, da diese keine Werte enthält
       * */
    	
      if(line < 1){
    	  line++;
    	  return;
      }
      
      DoubleWritable cashAmount = new DoubleWritable(1);
   
      String line = value.toString();
      String[] values = line.split(",");
       
      /**
       * Einlesen der Kilomenter
       * Einlesen des Gesamtbetrags
       * */
      double km = Double.parseDouble(values[9]);
      double amount = Double.parseDouble(values[values.length-1]);
      
      /**
       * Plausibilitätscheck
       * */
      if(km <= 0.0  || amount < 0.0){
    	  return;
      }
      
      /**
       * Dazufügen der Werten zu einem Key
       * 30km
       * 50km
       * Overall (immer)
       * */
      cashAmount.set(amount);
      if(km < 30){
    	  output.collect(new Text("30km"), cashAmount);
      }
      if(km < 50){
    	  output.collect(new Text("50km"), cashAmount);
      }
      
      output.collect(new Text("Overall"), cashAmount);
    }

  }
