package assignment

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import annotation.tailrec

import java.io._

import java.util.Date
import java.text.SimpleDateFormat
import java.util.Calendar
import Math._

import assignment.Season.Season


object Season extends Enumeration {
  type Season = Value
  val WINTER,SPRING,SUMMER,AUTUMN = Value
}




case class Photo(id: String,
                 latitude: Double,
                 longitude: Double,
                 datetime: Date)


object Flickr extends Flickr {

  @transient lazy val conf: SparkConf = new SparkConf().setMaster("local").setAppName("NYPD")
  @transient lazy val sc: SparkContext = new SparkContext(conf)

  /** Main function */
  def main(args: Array[String]): Unit = {

    val lines   = sc.textFile("src/main/resources/photos/elbow.csv").mapPartitions(_.drop(1))
    val raw     = rawPhotos(lines)

    val initialMeans = raw.takeSample(withReplacement = false,kmeansKernels).map(x => (x.latitude,x.longitude))
    val means   = kmeans(initialMeans, raw)

    val meansFile = new PrintWriter(new File("src/main/resources/photos/means.csv" ))
    means.foreach(x=>meansFile.println(x._1.toString + "," + x._2.toString))
    meansFile.close()

    save_csv(means,"means.csv")

    // THIRD DIMENSION

    val lines_season   = sc.textFile("src/main/resources/photos/flickr3D.csv").mapPartitions(_.drop(1))
    val raw_season     = rawPhotos(lines_season)


    val rawWinter = splitSeason(raw_season,Season.WINTER)
    val rawSpring = splitSeason(raw_season,Season.SPRING)
    val rawSummer = splitSeason(raw_season,Season.SUMMER)
    val rawAutumn = splitSeason(raw_season,Season.AUTUMN)

    val seasons = Array((rawWinter,"winter.csv"),(rawSpring,"spring.csv"),(rawSummer,"summer.csv"),(rawAutumn,"autumn.csv"))

    for (season <- seasons){
      val initialMeansSeason = season._1.takeSample(withReplacement = false,kmeansKernels).map(x => (x.latitude,x.longitude))
      val means   = kmeans(initialMeansSeason, season._1)
      save_csv(means,season._2)
    }



    val elbow : Array[(Int,Double)]= (1 to 20).toArray.map(i => {
      val meansElbow = kmeans(raw.takeSample(withReplacement = false,i).map(x => (x.latitude,x.longitude)),raw)
      val sum = raw.map(x => (findClosest((x.latitude,x.longitude),meansElbow), x))
        .groupByKey
        .map(x=> distanceInMeters(meansElbow(x._1),x._2))
        .collect
        .sum
      (i,sum)
    })

    val elbowFile = new PrintWriter(new File("src/main/resources/photos/elbowResult.csv" ))
    elbow.foreach(x=>elbowFile.println(x._1.toString + "," + x._2.toString))
    elbowFile.close()
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

  def distanceInMeters(c1: (Double, Double), c2: Iterable[Photo]): Double = {
    var sum = 0.0
    c2.foreach(x => sum+=distanceInMeters(c1,(x.latitude,x.longitude)))
    sum
  }


  /** Save into csv file*/
  def save_csv(means:Array[(Double, Double)],filename : String) ={
    val pw = new PrintWriter(new File("src/main/resources/photos/"+filename))
    means.foreach(x=>pw.println(x._1.toString + "," + x._2.toString))
    pw.close()
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
    var format = new SimpleDateFormat("yyyy:MM:dd HH:mm:ss")
    Photo(id = a(0), latitude = a(1).toDouble, longitude = a(2).toDouble,datetime = format.parse(a(3)))
  })

  def distanceInMeters(means: Array[(Double, Double)], newMeans: Array[(Double, Double)]): Double = {
    assert(means.length == newMeans.length)
    var sum = 0d
    ((means zip newMeans) map {case (a, b) => distanceInMeters(a,b)}).foreach(sum += _)
    sum
  }

  /** Find season from time stamp **/

  def getSeason(timeStamp : Date): Season = {
    val cal = Calendar.getInstance
    cal.setTime(timeStamp)
    val month = cal.get(Calendar.MONTH)
    val season = month match {
      case 0 => 0
      case 1 => 0
      case 2 => 1
      case 3 => 1
      case 4 => 1
      case 5 => 2
      case 6 => 2
      case 7 => 2
      case 8 => 3
      case 9 => 3
      case 10 => 3
      case 11 => 0
    }
    Season(season)
  }

  def splitSeason(lines : RDD[Photo], season : Season): RDD[Photo]={
    lines.filter(s => getSeason(s.datetime)==season)
  }

  @tailrec final def kmeans(means: Array[(Double, Double)], vectors: RDD[Photo], iter: Int = 1): Array[(Double, Double)] = {
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
