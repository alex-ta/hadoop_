package abstracthadoop;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public abstract class CustomMapper<KeyType, ValueType> extends MapReduceBase implements Mapper<LongWritable, Text, KeyType, ValueType> {

	/**
	 * Abstract Mapper fasst die Gernerics zusammen und reduziert die Anzahl aus 2
	 * Beim Mapper besteht das erste Inputargument immer aus eine LongWriteable, der 
	 * Datensatznummer sowie eines Texts, der eine Zeile beinhaltet
	 * 
	 * */
	@Override
	public abstract void map(LongWritable arg0, Text arg1, OutputCollector<KeyType, ValueType> arg2, Reporter arg3) throws IOException;
	
}