package abstracthadoop;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;


public abstract class CustomCombiner <KeyType,ValueType> extends MapReduceBase implements Reducer<KeyType, ValueType, KeyType, ValueType> {

	/**
	 * Abstract Combiner fasst die Gernerics zusammen und reduziert die Anzahl aus 2
	 * */
	
	@Override
	public abstract void reduce(KeyType arg0, Iterator<ValueType> arg1, OutputCollector<KeyType, ValueType> arg2, Reporter arg3) throws IOException;
	

}
