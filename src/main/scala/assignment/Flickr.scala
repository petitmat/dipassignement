package assignment

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import annotation.tailrec
import scala.reflect.ClassTag

import java.io.StringReader
import com.opencsv.CSVReader

import java.util.Date
import java.text.SimpleDateFormat
import Math._

case class Photo(id: String,
                 latitude: Double,
                 longitude: Double)
//datetime: Date)


object Flickr extends Flickr {

  @transient lazy val conf: SparkConf = new SparkConf().setMaster("local").setAppName("NYPD")
  @transient lazy val sc: SparkContext = new SparkContext(conf)

  /** Main function */
  def main(args: Array[String]): Unit = {

    val lines   = sc.textFile("src/main/resources/photos/dataForBasicSolution.csv")
    val raw     = rawPhotos(lines)

    val initialMeans = ???
    val means   = kmeans(initialMeans, raw)


  }
}


class Flickr extends Serializable {

  /** K-means parameter: Convergence criteria */
  def kmeansEta: Double = 20.0D

  /** K-means parameter: Number of clusters */
  def kmeansKernels = 16


  /** K-means parameter: Maximum iterations */
  def kmeansMaxIterations = 50

  //(lat, lon)
  def distanceInMeters(c1: (Double, Double), c2: (Double, Double)) = {
    val R = 6371e3
    val lat1 = toRadians(c1._1)
    val lon1 = toRadians(c1._2)
    val lat2 = toRadians(c2._1)
    val lon2 = toRadians(c2._2)
    val x = (lon2-lon1) * Math.cos((lat1+lat2)/2);
    val y = (lat2-lat1);
    Math.sqrt(x*x + y*y) * R;
  }


  /** Return the index of the closest mean */
  def findClosest(p: (Double, Double), centers: Array[(Double, Double)]): Int = {
    var bestIndex = 0
    var closest = Double.PositiveInfinity
    for (i <- 0 until centers.length) {
      val tempDist = distanceInMeters(p, centers(i))
      if (tempDist < closest) {
        closest = tempDist
        bestIndex = i
      }
    }
    bestIndex
  }

  /** Average the vectors */
  def averageVectors(ps: Iterable[Photo]): (Double, Double) = ???

  def rawPhotos(lines: RDD[String]): RDD[Photo] = ???


  @tailrec final def kmeans(means: Array[(Double, Double)], vectors: RDD[Photo], iter: Int = 1): Array[(Double, Double)] = ???

}