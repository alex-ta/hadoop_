package hadoopQ3;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

import abstracthadoop.CustomMapper;
import abstracthadoop.Validator;

public class Mapper extends CustomMapper<Text, DoubleWritable> {
   
	private static int line = 0;
	
    public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
      
    	/**
    	 * Ignorieren der ersten Line da diese keine Daten enthält
    	 * 
    	 * */
    	
      if(line < 1){
    	  line++;
    	  return;
      }
      
      Validator valid = new Validator();
      
      String line = value.toString();
      String[] values = line.split(",");
      
      
      /**
       * Einlesen der Werte gefahrene KM und vergangene Sekunden
       * 
       * */
      
      double km = Double.parseDouble(values[9]);
      long secs = Long.parseLong(values[8]);
      
      //double longitudeDrop = Double.parseDouble(values[12]);
      //double latitudeDrop = Double.parseDouble(values[13]);
      
      /**
       * Plausibilitätscheck
       * 
       * */
      
      if(!valid.isPosValue(km) || !valid.isPosValue(secs)){
    	  return;
      }
      
      /**
       * Umrechnung in kmh
       * */
      double kmH = (km/secs)*3600;
      
      output.collect(new Text("Average"), new DoubleWritable(kmH));
          
    }

  }
