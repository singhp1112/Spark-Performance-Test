package com.prakash.spark.test.performance;

/**
* @author psingh
*/
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

/**
 * RDDGroupByTest - Test performance of group by operation on data using spark's
 * RDD APIs Data set - https://www.kaggle.com/deepmatrix/imdb-5000-movie-dataset
 * Data is duplicated multiple times to make it larger (approx. > 1M)
 * Goal is to find average rating per language. Consider only those movies which
 * are color and whose number of critic reviews are more than 100
 */
public class RDDGroupByTest {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("GroupBy test with Spark RDD")
				.set("spark.eventLog.enabled", "true");
		JavaSparkContext jsc = new JavaSparkContext(conf);

		// read csv data as text file

		JavaRDD<String> rddInput = jsc.textFile("/Users/psingh/Documents/updated_movie_metadata.csv"); 
		// replace with your file path

		// remove first line of csv file
		String firstLine = rddInput.first();

		JavaRDD<String> skipFirstLine = rddInput.filter(new Function<String, Boolean>() {
			
			private static final long serialVersionUID = 1L;

			public Boolean call(String s) {

				// System.out.println(splitString[2]);

				if (!s.equals(firstLine)) {
					return true;
				}
				return false;
			}
		});

		//create RDD of string array
		
		JavaRDD<String[]> splitRDD = skipFirstLine.map(new Function<String,String[]>(){
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			public String[] call(String str){
				return str.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
			}
		});
		
		JavaRDD<String[]> filteredInput = splitRDD.filter(new Function<String[], Boolean>() {
			/**
			 * Select only color movies and whose critic reviews are more than
			 * 100
			 */
			private static final long serialVersionUID = 1L;

			public Boolean call(String[] splitString) {

				// System.out.println(splitString[2]);
				Integer numCritics = StringUtils.isNotBlank(splitString[2]) ? Integer.parseInt(splitString[2]) : 0;
				if (splitString[0].equals("Color") && numCritics > 100) {
					return true;
				}
				return false;
			}
		});

		// create pair RDD

		JavaPairRDD<String, Float> pairInputRDD = filteredInput.mapToPair(new PairFunction<String[], String, Float>() {
			/**
			 * Create pair RDD for group by operations
			 */
			private static final long serialVersionUID = 1L;

			public Tuple2<String, Float> call(String[] inputArray) {
				String lang = inputArray[19];
				Float score = Float.parseFloat(inputArray[25]);
				return new Tuple2<String, Float>(lang, score);
			}
		});

		// map values with 1

		JavaPairRDD<String, Tuple2<Float, Integer>> mappedInputData = pairInputRDD
				.mapValues(new Function<Float, Tuple2<Float, Integer>>() {
					/**
					 * Map values with one so number can counted for average
					 * operation
					 */
					private static final long serialVersionUID = 1L;

					public Tuple2<Float, Integer> call(Float flt) {
						return new Tuple2<Float, Integer>(flt, 1);
					}
				});

		// reduce by key

		JavaPairRDD<String, Tuple2<Float, Integer>> reducedInputData = mappedInputData
				.reduceByKey(new Function2<Tuple2<Float, Integer>, Tuple2<Float, Integer>, Tuple2<Float, Integer>>() {
					/**
					 * Add all RDD elements per key(language)
					 */
					private static final long serialVersionUID = 1L;

					public Tuple2<Float, Integer> call(Tuple2<Float, Integer> tpl1, Tuple2<Float, Integer> tpl2) {
						Float resVal = tpl1._1 + tpl2._1;
						Integer count = tpl1._2 + tpl2._2;
						return new Tuple2<Float, Integer>(resVal, count);

					}
				});

		// find average per language

		JavaPairRDD<String, Float> resultData = reducedInputData
				.mapValues(new Function<Tuple2<Float, Integer>, Float>() {
					/**
					 * Perform average operation on Tuple value of pair data
					 */
					private static final long serialVersionUID = 1L;

					public Float call(Tuple2<Float, Integer> inTple) {
						Float avgVal = inTple._1 / inTple._2;
						return avgVal;
					}
				});

		/*
		 * filteredInput.foreach(new VoidFunction<String>(){ public void
		 * call(String s){ System.out.println(s); } });
		 */
		/*
		 * reducedInputData.foreach( new
		 * VoidFunction<Tuple2<String,Tuple2<Float,Integer>>>(){ public void
		 * call(Tuple2<String,Tuple2<Float,Integer>> res){
		 * System.out.println(res); } });
		 */
		resultData.foreach(new VoidFunction<Tuple2<String, Float>>() {
			/**
			 * Display result by iterating over RDD
			 */
			private static final long serialVersionUID = 1L;

			public void call(Tuple2<String, Float> t) {
				System.out.println(t);
			}
		});

		jsc.close();
	}
}
