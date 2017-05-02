package com.prakash.spark.test.performance;

/**
* @author psingh
*/

import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class DfGroupByTest {

	public static void main(String[] args) {
		/**
		 * DfGroupByTest - Test performance of group by operation on data using spark's
		 * Dataframe APIs (Dataset of Row objects) 
		 * Data set - https://www.kaggle.com/deepmatrix/imdb-5000-movie-dataset
		 * Goal is to find average rating per language. Consider only those movies which
		 * are color and whose number of critic reviews are more than 100
		 */
		
		SparkSession spark = SparkSession.builder()
				.master("local")
				.appName("Dataframe Group By Test")
				.config("spark.eventLog.enabled", "true")
				.getOrCreate();
		
		//create Dataset<Row>
		
		Dataset<Row> inputDataset = spark.read()
				.format("csv")
				.option("header", true)
				.option("inferSchema", true)
				.load("/Users/psingh/Documents/movie_metadata.csv");
		
		//dtst.printSchema();
		//register this dataset as a temp table
		inputDataset.createOrReplaceTempView("tempMovieTable");
		
		//write SQL queries to perform group by and filtering operation
		Dataset<Row> queryResult = spark
				.sql(
						"select "
						+ "language, "
						+ "avg(imdb_score) "
						+ "from tempMovieTable "
						+ "where num_critic_for_reviews > 100 "
						+ "and color=\"Color\" "
						+ "group by language "
						);
		
		//Display result
		queryResult.foreach(new ForeachFunction<Row>() {
			
			/**
			 * Display each Row by iterating over RDD
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Row arg0) throws Exception {
				System.out.println(arg0.toString());
				
			}
		});
		spark.close();
	}

}
