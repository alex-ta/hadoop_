package hadoopQ10;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

import abstracthadoop.CustomMapper;
import abstracthadoop.Validator;

public class Mapper extends CustomMapper<Text, DoubleWritable> {
    // Mehr Geld im Norden oder Süden 
	// Mehr Trinkgeld Norden oder Süden
	
	private static int line = 0;
	
    public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
	  /**
       * Ignorieren der ersten Eingabe, da diese keine Werte enthält
       * */
    	
       if(line < 1){
    	  line++;
    	  return;
       }
      
      Validator valid = new Validator();
      
      String line = value.toString();
      String[] values = line.split(",");
      
      /**
       * Einlesen der Werte Trinkgeld, Gesamtbetrag und der Latitude
	   *
       * */
      double tip = Double.parseDouble(values[values.length-3]);
      double amount = Double.parseDouble(values[values.length-1]);
      //double longitudePick = Double.parseDouble(values[10]);
      double latitudePick = Double.parseDouble(values[11]);
      //double longitudeDrop = Double.parseDouble(values[12]);
      //double latitudeDrop = Double.parseDouble(values[13]);
      

      /**
       * Plausibilitätscheck
       * */
      
      if(!valid.isPosValue(tip)){
    	  return;
      }
      
      /**
       * Check ob die Fahrt im Süden oder Norden Startet
       * dazufügen zu dem jeweiligen Keyword
       * */
      
      if(valid.isBiggerCenter(latitudePick)){
    	  output.collect(new Text("northTip"), new DoubleWritable(tip));
    	  output.collect(new Text("northTotalAmount"), new DoubleWritable(amount));
      } else {
    	  output.collect(new Text("southTip"), new DoubleWritable(tip)); 
    	  output.collect(new Text("southTotalAmount"), new DoubleWritable(amount));
      }
      
    }

  }
