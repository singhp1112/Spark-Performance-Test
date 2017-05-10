package com.prakash.spark.performance;

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
 * SimpleGroupByUsingRDD - Test performance of group by operation on data using
 * spark's RDD APIs. Data set -
 * https://www.kaggle.com/deepmatrix/imdb-5000-movie-dataset. Data is duplicated
 * multiple times to make it larger (approx. > 1M) for performance testing. In
 * this test only reduce by key operation is performed to avoid any RDD specific
 * optimization. Goal is to find total number of faces per language from entire
 * movie dataset.
 */
public class SimpleGroupByUsingRDD {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("Simple Group By Test Spark RDD")
				.set("spark.eventLog.enabled", "true");
		JavaSparkContext jsc = new JavaSparkContext(conf);

		// read csv data as text file
		// header of the file is manually removed for better comparison with
		// dataset
		JavaRDD<String> rddInput = jsc.textFile("/Users/psingh/Documents/tempFile.csv");
		// replace with your file path

		JavaRDD<String[]> splitRDD = rddInput.map(new Function<String, String[]>() {
			/**
			 * split string rdd to create string array rdd for each column
			 */
			private static final long serialVersionUID = 1L;

			public String[] call(String str) {
				return str.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
			}
		});

		// create pair RDD for Color and number of faces on poster

		JavaPairRDD<String, Integer> pairInputRDD = splitRDD.mapToPair(new PairFunction<String[], String, Integer>() {
			/**
			 * Create pair RDD for group by operations
			 */
			private static final long serialVersionUID = 1L;

			public Tuple2<String, Integer> call(String[] str) {
				Integer numFaces = StringUtils.isNotBlank(str[15]) ? Integer.parseInt(str[15]) : 0;
				return new Tuple2<String, Integer>(str[0], numFaces);
			}
		});

		// sum number of faces for each color type
		JavaPairRDD<String, Integer> reducedOutput = pairInputRDD
				.reduceByKey(new Function2<Integer, Integer, Integer>() {
					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					public Integer call(Integer x, Integer y) {
						return x + y;
					}
				});

		reducedOutput.foreach(new VoidFunction<Tuple2<String, Integer>>() {
			/**
			 * Display result
			 */
			private static final long serialVersionUID = 1L;

			public void call(Tuple2<String, Integer> tpl) {
				System.out.println("Key is: " + tpl._1 + " value is: " + tpl._2);
			}
		});

		jsc.close();
	}
}