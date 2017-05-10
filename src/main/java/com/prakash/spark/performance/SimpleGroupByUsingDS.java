package com.prakash.spark.performance;

/**
* @author psingh
*/

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SimpleGroupByUsingDS {

	/**
	 * SimpleGroupByUsingDS - Test performance of group by operation on data using
	 * spark's Dataset APIs. Data set -
	 * https://www.kaggle.com/deepmatrix/imdb-5000-movie-dataset. Data is duplicated
	 * multiple times to make it larger (approx. > 1M) for performance testing. In
	 * this test only reduce by key operation is performed to avoid any RDD specific
	 * optimization. Goal is to find total number of faces per language from entire
	 * movie dataset.
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		SparkSession spark = SparkSession.builder()
				.master("local")
				.appName("Simple Group By Test Spark Dataset")
				.config("spark.eventLog.enabled", "true")
				.getOrCreate();
		
		//create Dataset<Row>
		
		Dataset<Row> inputDataset = spark.read()
				.format("csv")
				.option("header", false) //header of the file is manually deleted
				.option("inferSchema", false)
				.load("/Users/psingh/Documents/tempFile.csv"); //replace with your file location
		
		//inputDataset.printSchema();
		
		//register this dataset as a temp table
		inputDataset.createOrReplaceTempView("tempMovieTable");
		
		//write sql query to get aggregated number of faces per color type. 
		// Use default column names
		Dataset<Row> queryResult = spark.sql("select _c0, sum(cast(_c15 as int)) as num from tempMovieTable group by _c0");
		
		//display result
		queryResult.show();
		spark.close();
	}

}