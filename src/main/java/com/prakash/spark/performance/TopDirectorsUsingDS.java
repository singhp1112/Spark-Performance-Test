package com.prakash.spark.performance;

/**
* @author psingh
*/

import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class TopDirectorsUsingDS {

	public static void main(String[] args) {

		/**
		 * TopDirectorsUsingDS - To find top 10 movie director of all times
		 * using IMDB data and Dataset API Dataset APIs Data set -
		 * https://www.kaggle.com/deepmatrix/imdb-5000-movie-dataset. Data is
		 * duplicated multiple times to make it larger (approx. > 1M) for
		 * performance testing. Below are the criteria for selecting top 10 movie
		 * director
		 */

		SparkSession spark = SparkSession.builder().master("local").appName("Top Directors using Dataset API")
				.config("spark.eventLog.enabled", "true").config("spark.sql.crossJoin.enabled", "true").getOrCreate();

		// create Dataset<Row>

		Dataset<Row> inputDataset = spark.read().format("csv").option("header", true).option("inferSchema", true)
				.load("/Users/psingh/Documents/updated_movie_metadata.csv"); //replace your file location

		// inputDataset.printSchema();

		// register this dataset as a temp table
		inputDataset.createOrReplaceTempView("tempMovieTable");

		// write SQL query to default empty string to integer or double and
		// filter color movies
		Dataset<Row> queryResult = spark.sql(

				"select " + "director_name, " + "coalesce(num_critic_for_reviews,0) as numCritics, "
						+ "coalesce(director_facebook_likes,0) as dirFacebookLikes,"
						+ "coalesce(gross,0) as grossRevenue," + "coalesce(num_voted_users,0) as numVotedUsers,"
						+ "coalesce(cast_total_facebook_likes,0) as castFacebookLikes,"
						+ "coalesce(num_user_for_reviews,0) as numUsersReview," + "coalesce(budget,0) as totalBudget,"
						+ "coalesce(imdb_score,0.0) as imdbScore, "
						+ "coalesce(movie_facebook_likes,0) as movieFacebookLike " + "from tempMovieTable "
						+ "where color=\"Color\" "

		);

		// save resule of query to a new temp table
		queryResult.createOrReplaceTempView("tempMovieTable2");

		// write sql query to get sum on movie level
		Dataset<Row> queryResult2 = spark.sql("Select director_name," + "imdbScore,"
				+ "(imdbScore * (numCritics + numVotedUsers + numUsersReview )) as aggScore,"
				+ "(numCritics + numVotedUsers + numUsersReview ) as  aggReviews,"
				+ "Case when totalBudget=0 then 0 else ((grossRevenue - totalBudget)/totalBudget) end as percentProfit,"
				+ "dirFacebookLikes," + "castFacebookLikes," + "movieFacebookLike "
				+ "from tempMovieTable2 where numCritics > 100 ");

		// save result to a temp table
		queryResult2.createOrReplaceTempView("tempMovieTable3");

		// write sql query to get aggregated values on director level
		Dataset<Row> queryResult3 = spark.sql("select director_name," + "sum(aggScore) as totalScore,"
				+ "sum(aggReviews) as totalReviews," + "avg(percentProfit) as avgPercentProfit,"
				+ "avg(dirFacebookLikes) as avgDirFacebookLikes," + "avg(castFacebookLikes) as avgCastFacebookLikes,"
				+ "avg(movieFacebookLike) as avgMovieFacebookLikes " + "from tempMovieTable3 "
				+ "group by director_name");

		// save result of query to a temp table
		queryResult3.createOrReplaceTempView("tempMovieTable4");

		// find max values for each metrics
		Dataset<Row> maxQueryResult = spark.sql("select " + "max(imdbScore) as maxImdbScore,"
				+ "max(percentProfit) as maxPercentProfit," + "max(dirFacebookLikes) as maxDirFacebookLikes,"
				+ "max(castFacebookLikes) as maxCastFacebookLikes," + "max(movieFacebookLike) as maxMovieFacebookLike "
				+ "from tempMovieTable3 ");

		// save max value dataset to a temp table
		maxQueryResult.createOrReplaceTempView("maxValues");

		// cross join max value table to aggregated dataset to have max value
		// against each metric
		Dataset<Row> crossJoined = spark.sql("select * from tempMovieTable4 cross join maxValues");

		crossJoined.createOrReplaceTempView("tempMovieTable5");

		// write sql to get score on scale of 100 for each metric
		Dataset<Row> queryResult4 = spark.sql("select director_name,"
				+ "Case when totalReviews=0 then 0 else (((totalScore/totalReviews)/maxImdbScore)*100) end as wtScore,"
				+ "case when maxPercentProfit=0 then 0 else ((avgPercentProfit/maxPercentProfit)*100) end as profitScore,"
				+ "case when maxDirFacebookLikes=0 then 0 else ((avgDirFacebookLikes/maxDirFacebookLikes)*100) end as dirFacebookScore,"
				+ "case when maxCastFacebookLikes=0 then 0 else ((avgCastFacebookLikes/maxCastFacebookLikes)*100) end as castFacebookScore,"
				+ "case when maxMovieFacebookLike=0 then 0 else ((avgMovieFacebookLikes/maxMovieFacebookLike)*100) end as movieFacebookScore "
				+ "from tempMovieTable5");

		// save result to a temp table
		queryResult4.createOrReplaceTempView("tempMovieTable6");

		// write sql to add all scores for each director
		Dataset<Row> queryResult5 = spark.sql("select director_name,"
				+ "(wtScore+profitScore+dirFacebookScore+castFacebookScore+movieFacebookScore) as totalScore "
				+ "from tempMovieTable6 " + "order by totalScore desc limit 10");

		// crossJoined.printSchema();

		// Display result

		queryResult5.foreach(new ForeachFunction<Row>() {
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Row arg0) throws Exception {
				System.out.println(arg0.toString());
			}
		});

		spark.close();

	}

}