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
import scala.util.Try


case class Photo(id: String,
                 latitude: Double,
                 longitude: Double)
//datetime: Date)


object Flickr extends Flickr {

  @transient lazy val conf: SparkConf = new SparkConf().setMaster("local").setAppName("NYPD")
  @transient lazy val sc: SparkContext = new SparkContext(conf)

  /** Main function */
  def main(args: Array[String]): Unit = {

    val lines   = sc.textFile("src/main/resources/photos/flickrDirtySimple.csv").mapPartitions(_.drop(1))
    val raw     = rawPhotos(lines)

    val initialMeans = raw.takeSample(withReplacement = false,kmeansKernels).map(x => (x.latitude,x.longitude))
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
  def distanceInMeters(c1: (Double, Double), c2: (Double, Double)): Double = {
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

  def rawPhotos(lines: RDD[String]): RDD[Photo] = lines.filter(s => "^[0-9]{11}, [0-9]{2,3}.[0-9]*, [0-9]{2,3}.[0-9]*, [0-9]{4}(:[0-9]{2}){2} ([0-9]{2}:){2}[0-9]{2}$".r.findFirstIn(s).isDefined).map(l => {
    val a = l.split(",")
    println(l)
    Photo(id = a(0), latitude = a(1).toDouble, longitude = a(2).toDouble)
  })

  def distanceInMeters(means: Array[(Double, Double)], newMeans: Array[(Double, Double)]): Double = {
    assert(means.length == newMeans.length)
    var sum = 0d
    ((means zip newMeans) map {case (a, b) => distanceInMeters(a,b)}).foreach(sum += _)
    sum
  }

  @tailrec final def kmeans(means: Array[(Double, Double)], vectors: RDD[Photo], iter: Int = 1): Array[(Double, Double)] = {
    println(iter)
    val newMeans = means.clone() //copy initialMeans in a new array
    vectors.map(x => (findClosest((x.latitude, x.longitude), means), x))//map all the photos to find the closest center and return RDD[(index, Photo)]
      .groupByKey// group this RDD by index and return RDD[(index,Iterable[Photo])]
      .map(x => (x._1,averageVectors(x._2)))// map the new RDD and calculate the new centers of each Iterable[Photo] return RDD[(index,(Double,Double))]
      .collect//convert this RDD in an array Array[(index,(Double,Double))]
      .foreach(x => {
        newMeans.update(x._1,x._2) //go through this Array and update newMeans(index) with latitude and longitude of the new center
      })

    val distance = distanceInMeters(means, newMeans) //calculate total distance in meters between initial centers and new centers (to check the convergence criteria).

    if (distance < kmeansEta) //if convergence criteria is reached we break the recursivity
      newMeans
    else if (iter < kmeansMaxIterations)// else if the maxIterations criteria is not reached, we call recursively the function with newMeans
      kmeans(newMeans, vectors, iter + 1)
    else {
      println("Reached max iterations!")
      newMeans//if maxIterations criteria is reached we break the recursivity
    }
  }

}
