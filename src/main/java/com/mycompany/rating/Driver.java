/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Project/Maven2/JavaApp/src/main/java/${packagePath}/${mainClassName}.java to edit this template
 */

package com.mycompany.rating;

import java.util.ArrayList;
import java.util.Comparator;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;

import java.util.List;
import scala.Serializable;

/**
 *
 * @author mahmouderradi
 */
public class Driver {

        private static final String MASTER_URL = "spark://127.0.0.1:7077";
        private static final String APP_NAME = "Rating";
	private static final int N = 5;
	private static final double WEAK_RATING_THRESHOLD = 3.0;
	

        public static void main(String[] args) {
                SparkConf conf = new SparkConf()
			.setAppName(APP_NAME)
			.setMaster(MASTER_URL);
                JavaSparkContext sc = new JavaSparkContext(conf);

                List<Rating> ratings = new ArrayList<>();

                ratings.add(new Rating("11QER/31", 1));
		ratings.add(new Rating("13-Q2/P2", 4));
		ratings.add(new Rating("14-Q1/L3", 3));
		ratings.add(new Rating("1546-QQ2", 3));
		ratings.add(new Rating("1558-QW1", 3));
		ratings.add(new Rating("2232/QTY", 2));
		ratings.add(new Rating("2232/QWE", 2));
		ratings.add(new Rating("2238/QPD", 2));
		ratings.add(new Rating("23109-HB", 3));
		ratings.add(new Rating("23114-AA", 1));
		ratings.add(new Rating("54778-2T", 4));
		ratings.add(new Rating("89-WRE-Q", 1));
		ratings.add(new Rating("PVC23DRT", 5));
		ratings.add(new Rating("SM-18277", 5));
		ratings.add(new Rating("SW-23116", 5));
		ratings.add(new Rating("WR3/TT3", 1));
		
		JavaRDD<Rating> ratingRDD = sc.parallelize(ratings);
		
		JavaPairRDD<String, Integer> indexedRatingRDD = ratingRDD
			.mapToPair(rating -> new Tuple2<>(rating.getProductId(), rating.getStars()));
		
		JavaPairRDD<String, Integer> ratingSumByProductRDD = indexedRatingRDD.reduceByKey((a, b) -> a + b);
		
		JavaPairRDD<String, Integer> ratingCountByProductRDD = indexedRatingRDD
			.mapToPair(indexedRating -> new Tuple2<>(indexedRating._1, 1))
			.reduceByKey((a, b) -> a + b);
		
		JavaPairRDD<String, Tuple2<Integer, Integer>> ratingSumAndCountByProductRDD = ratingSumByProductRDD
			.join(ratingCountByProductRDD);
		
		JavaPairRDD<String, Double> averageRatingByProductRDD = ratingSumAndCountByProductRDD
			.mapToPair(sumAndCountByProduct -> new Tuple2<>(sumAndCountByProduct._1,
				(double) sumAndCountByProduct._2._1 / sumAndCountByProduct._2._2));
		
		JavaPairRDD<String, Double> weaklyRatedProductsRDD = averageRatingByProductRDD 
			.filter(productRating -> productRating._2 < WEAK_RATING_THRESHOLD);
		
		List<Tuple2<String, Double>> weaklyRatedProducts = weaklyRatedProductsRDD.collect();
		System.out.println("Weakly-rated products:");
		for (Tuple2<String, Double> weaklyRatedProduct : weaklyRatedProducts) {
			System.out.println(weaklyRatedProduct._1 + " : " + weaklyRatedProduct._2);
		}
		
		List<Tuple2<String, Double>> topRatedProducts = averageRatingByProductRDD.takeOrdered(N,
			new ProductRatingComparator());
		
		System.out.println("\nTop-" + N + " rated products:");
		for (Tuple2<String, Double> topRatedProduct : topRatedProducts) {
			System.out.println(topRatedProduct._1 + " : " + topRatedProduct._2);
		}
		
		double meanRating = averageRatingByProductRDD.mapToDouble(Tuple2::_2).mean(); 
		double stdDevRating = averageRatingByProductRDD.mapToDouble(Tuple2::_2).stdev();
		
		System.out.println("\nMean rating: " + meanRating);
		System.out.println("Standard deviation of ratings: " + stdDevRating);
		
		sc.stop();
	}
	
	private static class ProductRatingComparator implements Comparator<Tuple2<String, Double>>, Serializable {
		@Override
		public int compare(Tuple2<String, Double> o1, Tuple2<String, Double> o2) {
			return -Double.compare(o1._2, o2._2);
		}
	}
}