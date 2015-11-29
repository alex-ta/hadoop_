package hadoopQ9;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

import abstracthadoop.CustomMapper;
import abstracthadoop.Validator;

public class Mapper extends CustomMapper<DoubleWritable, DoubleWritable> {
	// Mehr Gewinn pro km
	
	private static int line = 0;
	
    public void map(LongWritable key, Text value, OutputCollector<DoubleWritable, DoubleWritable> output, Reporter reporter) throws IOException {
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
       * Einlesend er Werte Gesamtbetrag und KM
       * Runden der Werte um einheitliche Schlüssel zu erhalten
       * */
      
      double amount = Double.parseDouble(values[values.length-1]);
      double km = Math.ceil(Double.parseDouble(values[9]));

      
      /**
       * Plausibilitätscheck
       */
      
      if(!valid.isPosValue(amount) || !valid.isPosValue(km)){
    	  return;
      }
      
      output.collect(new DoubleWritable(km), new DoubleWritable(amount));
    }

  }
