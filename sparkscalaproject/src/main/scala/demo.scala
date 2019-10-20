import java.io.PrintWriter
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel
import ucar.nc2._


import scala.collection.JavaConverters._

object demo {
  def open(uri: String) = {
    if (uri.startsWith("E:")) {

      val raf = new ucar.unidata.io.MMapRandomAccessFile(uri, "r")
      NetcdfFile.open(raf, uri, null, null)
    } else {
      NetcdfFile.open(uri)
    }
  }

  def main(args:Array[String]): Unit = {
    val netcdfUri =
      if (args.size > 0) args(0)
      else "E:/MTECH/software/sresa1b_ncar_ccsm3-example.nc"
    val ncfile = open(netcdfUri)
    val vs = ncfile.getDetailInfo()
    val pw = new PrintWriter("E:/MTECH/software/netcdf1.txt")
    pw.print(vs)
    pw.close()

//    val conf = new SparkConf()
//    conf.setMaster("local")
//    conf.setAppName("FirstApp")

  //  val sc = new SparkContext(conf)
    //val ncfile = open(netcdfUri)

//    val rdd = sc.makeRDD(vs)
//    println(rdd.collect())
    // print(vs)
//    sc.stop()
  }

}
