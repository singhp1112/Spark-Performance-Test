package com.prakash.spark.performance;

/**
* @author psingh
*/

import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.DoubleFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

/**
 * TopDirectorsUsingRDD - To find top 10 movie director of all times using IMDB
 * data and RDD API.
 * RDD APIs Data set -
 * https://www.kaggle.com/deepmatrix/imdb-5000-movie-dataset. Data is duplicated
 * multiple times to make it larger (approx. > 1M) for performance testing. Below
 * are the criteria for selecting top 10 movie director
 * 
 */
public class TopDirectorsUsingRDD {

	public static void main(String[] args) {

		SparkConf conf = new SparkConf().setMaster("local").setAppName("Top Directors using RDD API")
				.set("spark.eventLog.enabled", "true");
		JavaSparkContext jsc = new JavaSparkContext(conf);

		// read csv data as text file

		JavaRDD<String> rddInput = jsc.textFile("/Users/psingh/Documents/updated_movie_metadata.csv");
		// replace with your file path

		// remove first line of csv file
		String firstLine = rddInput.first();

		JavaRDD<String> skipFirstLine = rddInput.filter(new Function<String, Boolean>() {

			// Skip header of the CSV file

			private static final long serialVersionUID = 1L;

			public Boolean call(String s) {

				// System.out.println(splitString[2]);

				if (!s.equals(firstLine)) {
					return true;
				}
				return false;
			}
		});

		// create RDD of string array

		JavaRDD<String[]> splitRDD = skipFirstLine.map(new Function<String, String[]>() {
			/**
			 * Split string based on regex
			 */
			private static final long serialVersionUID = 1L;

			public String[] call(String str) {
				return str.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
			}
		});

		// filter RDD to include only color movies and where number of critic is
		// more than 100

		JavaRDD<String[]> filteredInput = splitRDD.filter(new Function<String[], Boolean>() {
			/**
			 * Select only color movies and whose critic reviews are more than
			 * 100
			 */
			private static final long serialVersionUID = 1L;

			public Boolean call(String[] splitString) {
				Integer numCritics = StringUtils.isNotBlank(splitString[2]) ? Integer.parseInt(splitString[2]) : 0;
				if (splitString[0].equals("Color") && numCritics > 100) {
					return true;
				}
				return false;
			}
		});

		// cache the filteredInput RDD as this will be used multiple times
		filteredInput.cache();

		// find weighted imdb score, weighted against number of critics,
		// number of voted users, and number of users

		// weighted imdb score step 1 - calculate imdb score multiplied by total
		// of number of critics, number of voted users,
		// and number of users per movie
		JavaPairRDD<String, Tuple2<Double, Double>> wtScoreInput = filteredInput
				.mapToPair(new PairFunction<String[], String, Tuple2<Double, Double>>() {
					/**
					 * Converts to key value pair RDD
					 */
					private static final long serialVersionUID = 1L;

					public Tuple2<String, Tuple2<Double, Double>> call(String[] str) {
						String key = str[1].trim();
						Double numCtritic = StringUtils.isNotBlank(str[2]) ? Double.parseDouble(str[2]) : 0;
						Double numVoted = StringUtils.isNotBlank(str[12]) ? Double.parseDouble(str[12]) : 0;
						Double numUsers = StringUtils.isNotBlank(str[18]) ? Double.parseDouble(str[18]) : 0;
						Double score = StringUtils.isNotBlank(str[25]) ? Double.parseDouble(str[25]) : 0;
						Double tupleVal = numCtritic + numVoted + numUsers;
						Double tupleKey = score * tupleVal;

						return new Tuple2<String, Tuple2<Double, Double>>(key,
								new Tuple2<Double, Double>(tupleKey, tupleVal));
					}
				});

		// weighted imdb score step 2 - sum above scores by director
		JavaPairRDD<String, Tuple2<Double, Double>> wtScoreSum = wtScoreInput
				.reduceByKey(new Function2<Tuple2<Double, Double>, Tuple2<Double, Double>, Tuple2<Double, Double>>() {
					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					public Tuple2<Double, Double> call(Tuple2<Double, Double> tpl1, Tuple2<Double, Double> tpl2) {
						Double outKey = tpl1._1 + tpl2._1;
						Double outVal = tpl1._2 + tpl2._2;
						return new Tuple2<Double, Double>(outKey, outVal);
					}
				});

		// weighted imdb score step 3 - calculate average weighted score
		JavaPairRDD<String, Double> wtScore = wtScoreSum.mapValues(new Function<Tuple2<Double, Double>, Double>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			public Double call(Tuple2<Double, Double> tpl) {
				Double res = 0.0;
				if (tpl._2 != 0.0) {
					res = tpl._1 / tpl._2;
				}
				return res;
			}
		});

		// System.out.println(wtScore.first());

		/**
		 * Find average percentage profit
		 * 
		 */
		// Average percent score step 1 - find percentage profit by using budget
		// and gross profit and create
		// a key value pair RDD for average

		JavaPairRDD<String, Tuple2<Double, Integer>> avgProfitInput = filteredInput
				.mapToPair(new PairFunction<String[], String, Tuple2<Double, Integer>>() {
					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					public Tuple2<String, Tuple2<Double, Integer>> call(String[] str) {
						String key = str[1].trim();
						// gross - budget
						Double gross = StringUtils.isNotBlank(str[8]) ? Double.parseDouble(str[8]) : 0;
						Double budget = StringUtils.isNotBlank(str[22]) ? Double.parseDouble(str[22]) : 0;
						Double profit = gross - budget;
						Double profitPercent = 0.0;
						if (budget != 0.0) {
							profitPercent = profit / budget;
						}
						return new Tuple2<String, Tuple2<Double, Integer>>(key,
								new Tuple2<Double, Integer>(profitPercent, 1));

					}
				});

		// Average percent score step 2 - sum percentage per movie by director.
		JavaPairRDD<String, Tuple2<Double, Integer>> avgProfitSum = avgProfitInput.reduceByKey(
				new Function2<Tuple2<Double, Integer>, Tuple2<Double, Integer>, Tuple2<Double, Integer>>() {
					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					public Tuple2<Double, Integer> call(Tuple2<Double, Integer> tpl1, Tuple2<Double, Integer> tpl2) {
						Double key = tpl1._1 + tpl2._1;
						Integer val = tpl1._2 + tpl2._2;
						return new Tuple2<Double, Integer>(key, val);
					}
				});

		// Average percent score step 3 -Calculate average by dividing total
		// percentage profit by running count
		JavaPairRDD<String, Double> profitAvg = avgProfitSum.mapValues(new Function<Tuple2<Double, Integer>, Double>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			public Double call(Tuple2<Double, Integer> tpl) {
				return tpl._1 / tpl._2;
			}
		});

		// System.out.println(profitAvg.first());

		// Find average director facebook likes

		// calculate average facebook like per director using custom function
		// mapAverageFunction
		JavaPairRDD<String, Tuple2<Double, Integer>> directorFbLikeMap = filteredInput
				.mapToPair(new MapAverageFunction(4) {

					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;
				});
		JavaPairRDD<String, Tuple2<Double, Integer>> directorFbLikeSum = directorFbLikeMap
				.reduceByKey(new ReduceAverageFunction() {

					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;
				});
		JavaPairRDD<String, Double> directorFbLikeAvg = directorFbLikeSum.mapValues(new CalAvgFunction() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;
		});
		// System.out.println(directorFbLikeAvg.first());

		// Find average total cast facebook likes

		// calculate average facebook like for total casts per director using
		// custom function mapAverageFunction
		JavaPairRDD<String, Tuple2<Double, Integer>> castFbLikeMap = filteredInput
				.mapToPair(new MapAverageFunction(13) {

					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;
				});
		JavaPairRDD<String, Tuple2<Double, Integer>> castFbLikeSum = castFbLikeMap
				.reduceByKey(new ReduceAverageFunction() {

					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;
				});
		JavaPairRDD<String, Double> castFbLikeAvg = castFbLikeSum.mapValues(new CalAvgFunction() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;
		});
		// System.out.println(castFbLikeAvg.first());

		// Find average total cast facebook likes

		// calculate average facebook like per movie per director using custom
		// function mapAverageFunction
		JavaPairRDD<String, Tuple2<Double, Integer>> movieFbLikeMap = filteredInput
				.mapToPair(new MapAverageFunction(27));
		JavaPairRDD<String, Tuple2<Double, Integer>> movieFbLikeSum = movieFbLikeMap
				.reduceByKey(new ReduceAverageFunction());
		JavaPairRDD<String, Double> movieFbLikeAvg = movieFbLikeSum.mapValues(new CalAvgFunction());
		// System.out.println(movieFbLikeAvg.first());

		// Find maximum values for all criteria to establish score

		// max weighted IMDB score for any movie

		JavaDoubleRDD wtScoreRDD = filteredInput.mapToDouble(new DoubleFunction<String[]>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			public double call(String[] str) {
				Double numCtritic = StringUtils.isNotBlank(str[2]) ? Double.parseDouble(str[2]) : 0;
				Double numVoted = StringUtils.isNotBlank(str[12]) ? Double.parseDouble(str[12]) : 0;
				Double numUsers = StringUtils.isNotBlank(str[18]) ? Double.parseDouble(str[18]) : 0;
				Double score = StringUtils.isNotBlank(str[25]) ? Double.parseDouble(str[25]) : 0;
				Double tupleVal = numCtritic + numVoted + numUsers + score;
				Double tupleKey = score * tupleVal;
				Double res = 0.0;
				if (tupleVal != 0) {
					res = tupleKey / tupleVal;
				}
				return res;
			}
		});

		Double maxWtScore = wtScoreRDD.max();
		// System.out.println("max score " + maxWtScore);

		// max percentage profit for any movie

		JavaDoubleRDD avgProfitRDD = filteredInput.mapToDouble(new DoubleFunction<String[]>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			public double call(String[] str) {
				Double gross = StringUtils.isNotBlank(str[8]) ? Double.parseDouble(str[8]) : 0;
				Double budget = StringUtils.isNotBlank(str[22]) ? Double.parseDouble(str[22]) : 0;
				Double profit = gross - budget;
				Double profitPercent = 0.0;
				if (budget != 0.0) {
					profitPercent = profit / budget;
				}
				return profitPercent;
			}
		});

		Double maxAvgProfit = avgProfitRDD.max();
		// System.out.println("max profit " + maxAvgProfit);

		// max director Facebook likes using custom function convertToDouble
		Double maxDirFacebook = filteredInput.mapToDouble(new ConvertToDouble(4)).max();
		// System.out.println("max " + maxDirFacebook);

		// max total cast Facebook likes using custom function convertToDouble
		Double maxCastFacebook = filteredInput.mapToDouble(new ConvertToDouble(13)).max();
		// System.out.println("max " + maxCastFacebook);

		// max movie Facebook likes using custom function convertToDouble
		Double maxMovieFacebook = filteredInput.mapToDouble(new ConvertToDouble(27)).max();
		// System.out.println("max " + maxMovieFacebook);

		// Get RDD of scores on scale of 100 using custom function
		// ScoreCalculate

		JavaPairRDD<String, Double> wtScoreScore = wtScore.mapValues(new ScoreCalculate(maxWtScore));
		JavaPairRDD<String, Double> avgProfitScore = profitAvg.mapValues(new ScoreCalculate(maxAvgProfit));
		JavaPairRDD<String, Double> dirFBLiketScore = directorFbLikeAvg.mapValues(new ScoreCalculate(maxDirFacebook));
		JavaPairRDD<String, Double> castFBLikeScore = castFbLikeAvg.mapValues(new ScoreCalculate(maxCastFacebook));
		JavaPairRDD<String, Double> mvFBScore = movieFbLikeAvg.mapValues(new ScoreCalculate(maxMovieFacebook));

		// Union all RDDs together
		JavaPairRDD<String, Double> unionRDD = wtScoreScore.union(avgProfitScore).union(dirFBLiketScore)
				.union(castFBLikeScore).union(mvFBScore);
		// System.out.println(unionRDD.first());

		// Add all scores for each director
		JavaPairRDD<String, Double> groupedUnionRDD = unionRDD.reduceByKey(new Function2<Double, Double, Double>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			public Double call(Double a, Double b) {
				return a + b;
			}
		});

		// System.out.println(groupedUnionRDD.first());

		// swap key value pair so it can sorted on key to get top director
		// scores
		JavaPairRDD<Double, String> finalResVal = groupedUnionRDD
				.mapToPair(new PairFunction<Tuple2<String, Double>, Double, String>() {
					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					public Tuple2<Double, String> call(Tuple2<String, Double> tp) {
						return tp.swap();
					}
				});

		List<Tuple2<Double, String>> sortedResult = finalResVal.sortByKey(false).take(10);

		// display results
		for (Tuple2<Double, String> tpl : sortedResult) {
			System.out
					.println("DIRECTOR NAME: " + tpl._2 + ", TOTAL SCORE: " + (double) Math.round(tpl._1 * 100) / 100);
		}

		// System.out.println(sortedResult.first());
		jsc.close();
	}

}

// Functions classes for RDD operation
class MapAverageFunction implements PairFunction<String[], String, Tuple2<Double, Integer>> {
	/**
	 * Creates key value pair RDD for given column number (0 based)
	 */
	private static final long serialVersionUID = 1L;
	private Integer index;

	public MapAverageFunction(Integer x) {
		this.index = x;
	}

	@Override
	public Tuple2<String, Tuple2<Double, Integer>> call(String[] t) throws Exception {
		String key = t[1].trim();
		Double val = StringUtils.isNotBlank(t[index]) ? Double.parseDouble(t[index]) : 0;
		;
		return new Tuple2<String, Tuple2<Double, Integer>>(key, new Tuple2<Double, Integer>(val, 1));
	}

}

// Reduce average function class
class ReduceAverageFunction
		implements Function2<Tuple2<Double, Integer>, Tuple2<Double, Integer>, Tuple2<Double, Integer>> {

	/**
	 * Reduce function for Tuple2<Double, Integer> elements
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public Tuple2<Double, Integer> call(Tuple2<Double, Integer> v1, Tuple2<Double, Integer> v2) throws Exception {
		Double key = v1._1 + v2._1;
		Integer val = v1._2 + v2._2;
		return new Tuple2<Double, Integer>(key, val);
	}

}

// Calculate average function class
class CalAvgFunction implements Function<Tuple2<Double, Integer>, Double> {

	/**
	 * Calculates average for a tuple2
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public Double call(Tuple2<Double, Integer> v1) throws Exception {

		return v1._1 / v1._2;
	}

}

// Class for converting string to double
class ConvertToDouble implements DoubleFunction<String[]> {
	/**
	 * Converts to double of given column number
	 */
	private static final long serialVersionUID = 1L;
	private Integer index;

	public ConvertToDouble(Integer val) {
		this.index = val;
	}

	@Override
	public double call(String[] t) throws Exception {
		Double val = StringUtils.isNotBlank(t[index]) ? Double.parseDouble(t[index]) : 0;
		;
		return val;
	}

}

// class for calucating score based on 100 scale
class ScoreCalculate implements Function<Double, Double> {

	/**
	 * Calculate score on scale of 100 based on max value provided
	 */
	private static final long serialVersionUID = 1L;
	private Double maxVal;

	public ScoreCalculate(Double v) {
		this.maxVal = v;
	}

	@Override
	public Double call(Double v1) throws Exception {
		return (v1 / maxVal) * 100;
	}

}