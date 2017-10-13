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

import scala.util.Random.shuffle

case class Photo(id: String,
                 latitude: Double,
                 longitude: Double)
//datetime: Date)


object Flickr extends Flickr {

  @transient lazy val conf: SparkConf = new SparkConf().setMaster("local").setAppName("NYPD")
  @transient lazy val sc: SparkContext = new SparkContext(conf)

  /** Main function */
  def main(args: Array[String]): Unit = {

    val lines   = sc.textFile("src/main/resources/photos/dataForBasicSolution.csv").mapPartitions(_.drop(1))
    val raw     = rawPhotos(lines)

    val initialMeans = raw.takeSample(false,kmeansKernels).map(x => (x.latitude,x.longitude))
    val means   = kmeans(initialMeans, raw)
    means.foreach(x=>println(x))
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
    val x = (lon2-lon1) * Math.cos((lat1+lat2)/2)
    val y = lat2-lat1
    Math.sqrt(x*x + y*y) * R
  }


  /** Return the index of the closest mean */
  def findClosest(p: (Double, Double), centers: Array[(Double, Double)]): Int = {
    var bestIndex = 0
    var closest = Double.PositiveInfinity
    for (i <- centers.indices) {
      val tempDist = distanceInMeters(p, centers(i))
      if (tempDist < closest) {
        closest = tempDist
        bestIndex = i
      }
    }
    bestIndex
  }

  /** Average the vectors */
  def averageVectors(ps: Iterable[Photo]): (Double, Double) = {
    var lat, long, size = 0.0
    ps.foreach(x => {
      lat += x.latitude
      long += x.longitude
      size += 1
    })
    (lat/size, long/size)
  }

  def rawPhotos(lines: RDD[String]): RDD[Photo] = lines.map(l => {val a = l.split(","); Photo(id = a(0), latitude = a(1).toDouble, longitude = a(2).toDouble)})

  def euclideanDistance(means: Array[(Double, Double)], newMeans: Array[(Double, Double)]) = {
    assert(means.length == newMeans.length)
    var sum = 0d
    ((means zip newMeans) map {case (a, b) => distanceInMeters(a,b)}).foreach(sum += _)
    sum
  }

  @tailrec final def kmeans(means: Array[(Double, Double)], vectors: RDD[Photo], iter: Int = 1): Array[(Double, Double)] = {
    println(iter)
    val newMeans = means.clone()
    vectors.map(x => (findClosest((x.latitude, x.longitude), means), x))
      .groupByKey
      .map(x => averageVectors(x._2))
      .collect
      .foreach(x => {
        newMeans.update(findClosest((x._1,x._2),means),x)
      })

    val distance = euclideanDistance(means, newMeans)

    if (distance < kmeansEta)
      newMeans
    else if (iter < kmeansMaxIterations)
      kmeans(newMeans, vectors, iter + 1)
    else {
      println("Reached max iterations!")
      newMeans
    }
  }

}
