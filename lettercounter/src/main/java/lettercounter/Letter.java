package lettercounter;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.util.Arrays;

public class Letter {
	
	 @SuppressWarnings({ "rawtypes", "unchecked" })
	private static void letterCount(String fileName) {
		 
		 	//filters error messages in console only to warn level
		    Logger.getLogger("org.apache").setLevel(Level.WARN);
		 
	        SparkConf conf = new SparkConf().setAppName("LetterCounter").setMaster("local[*]");

	        JavaSparkContext sc = new JavaSparkContext(conf);

	        JavaRDD<String> input = sc.textFile(fileName);
	        
	        //mapping lines
	        JavaRDD<String> getWords = input.flatMap(content -> Arrays.asList(content.split(" ")).iterator());
	        
	        
	        // character unicode check
	        JavaRDD<String> wordsFilter =getWords.filter(
	        		s -> !  s.equals("") &&  ! (s.charAt(0)<'\u0600') &&  
	        			 ! (s.charAt(0)>'\u06FF')
	        			 ); 
	        
	        //reducing pairs to first letter,count
	        JavaPairRDD result = wordsFilter.mapToPair(
	        		t -> new Tuple2(t.charAt(0), 1)).reduceByKey((value1, value2) -> (int) value1 + (int) value2
	        				);

	        result.saveAsTextFile("output");
	        sc.close();
	    }
	 	
	
	 
	public static void main(String[] args) {
		  letterCount("C:\\Users\\Sina\\Downloads\\ShamsDaftar4.txt");
		  
	}
}
