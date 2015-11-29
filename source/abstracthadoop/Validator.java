package abstracthadoop;

import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class Validator {
	/**
	 * Validator Klasse die Eingaben prüft 
	 * 
	 * */
	
	/**
	 * noString Array enthält alle Deklerationen die keinen Sinnvollen Wert enthalten
	 * patter ist ein vorcompiliertes Pattern der noString Variablen
	 * center ist ein gesetztes Zentrum
	 * hardBigger ist der größte erreichte Wert
	 * hardLess ider der kleinste erreichte Wert 
	 * */
	private String[] noString;
	private Pattern pattern;
	private double center;
	private double hardBigger; 
	private double hardLess;
	
	public Validator(double center){	
		noString = new String[]{"Null","NULL","null"};
		pattern = Pattern.compile("("+getString(noString,"|")+")");
		this.center = center;
	}
	
	public Validator(){
		this(0.0);
	}
	
	public void setTopBoarder(double border){
		this.hardBigger = border;
	}
	
	public void setDownBoarder(double border){
		this.hardLess = border;
	}
	
	/**
	 * Checkfunktionen
	 * isBigger prüft ob etwas größer als die Grenze ist
	 * isLess prüft ob etwas kleiner als die Grenze ist
	 * 
	 * */
	
	public boolean isBiggerCenter(double value){
		/**value > center*/
		return value > center;
	}
	
	public boolean isBiggerCenter(float value){
		/**value > center*/
		return value > center;
	}
	
	public boolean isBiggerCenter(int value){
		/**value > center*/
		return value > center;
	}
	
	public boolean isBigger(double value){
		/**value > border*/
		return value > hardBigger;
	}
	
	public boolean isLess(double value){
		/**value < border*/
		return value < hardLess;
	}

	public boolean isBigger(float value){
		/**value > border*/
		return value > hardBigger;
	}
	
	public boolean isLess(float value){
		/**value < border*/
		return value < hardLess;
	}
	
	public boolean isBigger(int value){
		/**value > border*/
		return value > hardBigger;
	}
	
	public boolean isLess(int value){
		/**value < border*/
		return value < hardLess;
	}
	
	public boolean isPosValue(double value){
		/**value > 0*/
		return value > 0;
	}
	
	public boolean isPosValue(float value){
		/**value > 0*/
		return value > 0.0f;
	}
	
	public boolean isPosValue(int value){
		/**value > 0*/
		return value > 0.0;
	}
	
	public boolean containsNoString(String value){
		/** Beinhaltet der String undefinierte Werte null | Null usw.*/
		for(String s :noString){
			if(s.equals(value)){
				return true;
			}
		}
		return false;
	}
	
	public boolean containsNoString(String[] values){
		/**Beinhaltet das String Array undefinierte Werte null | Null usw.*/
		Matcher matcher = pattern.matcher(getString(values));
		if(matcher.matches()){
			return true;
		}
		return false;
	}
	
	public String getString(String[] values, String seperator){
		/**Gibt eine Stringrepresentation eines String Arrays zurück*/
		StringBuilder builder = new StringBuilder();
		for(int i = 0; i < values.length; i++){
			if(i > 0){
				builder.append(seperator);
			}
			builder.append(values[i]);
		}
		return builder.toString();
	}
	
	public String getString(String[] values){
		/**Gibt eine Stringrepresentation eines String Arrays zurück*/
		return getString(values," ");
	}
	
}
