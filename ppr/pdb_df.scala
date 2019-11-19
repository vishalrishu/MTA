package api
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import java.io.File
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

object pdb_df {
  //case class PDB(frame_no:Int, index:Int, atom:String, res_label:String, res_count:Int,X:Double, Y:Double,Z:Double)
  def getListOfFiles(dir: String, spark: SparkSession): Array[String] = {
    val sc = spark.sparkContext
    var fileList = new scala.collection.mutable.ArrayBuffer[String]()
    val config = new Configuration()
    val path = new Path(dir)
    val fd = FileSystem.get(config).listStatus(path)
    fd.foreach(x => {
      fileList += x.getPath.toString()
    })
    return fileList.toArray
  }

   def getListOfFiles1(dir: String):List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
  }

  def main(args: Array[String]): Unit = {

    val time = System.currentTimeMillis()
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
//    val conf = new SparkConf().setAppName("MTA").set("spark.files.overwrite","true").setMaster("local")
//    val sc1 = new SparkContext(conf)
    val spark = SparkSession.builder.getOrCreate()
    val hconf = spark.sparkContext.hadoopConfiguration
    hconf.setInt("dfs.replication", 1)
    import spark.implicits._
    val sc = spark.sparkContext
    var r = new Read()
    val prm_file = args(0)
    val crd_file_dir = args(1)
    val out_dir = args(2)
    //println(crd_file_dir)

    var crd_files = getListOfFiles(crd_file_dir,spark).sorted

    var dfArr = new Array[Dataset[PDB]](crd_files.length)
    //var df1 : Dataset[PDB]= null
    var count: Int = 0
    var image = new Image(r.boxDim, spark)
    var arr = r.read_pointers(prm_file, spark)
    //val df = r.read_prm(prm_file, arr(0), arr(1), crd_file_dir, out_dir, 1, spark)
    //df.toJavaRDD.coalesce(1).saveAsTextFile("hdfs:///user/ppr.gp2/out/df")
    //dfArr(0) = r.read_prm(prm_file, arr(0), arr(1), crd_files(0), out_dir, 1, spark)
    //var df = r.read_prm(prm_file, arr(0), arr(1), crd_files(0), out_dir, 1, spark)
    //temp_df.repartition($"frame_no")
    //temp_df.toJavaRDD.saveAsTextFile("hdfs:///user/ppr.gp2/out/df")
  //  var mask = new Mask()

    for (i <- 0 to crd_files.length - 1)
    {
      var temp = r.read_prm(prm_file, arr(0), arr(1), crd_files(i).toString, out_dir, (i.toInt * 10)+1, spark)

      //for(j<-10*i + 1 to 10*i + 10) {
      var j = 10*i +1
      val df = image.autoImage(temp,j, r.firstMolCount)
//      df = df.collect()
      r.gen_pdb(j,df.collect(),arr(0), out_dir, spark)
      //}
    }
    //dfArr(0).toJavaRDD.coalesce(1).saveAsTextFile("hdfs:///user/ppr.gp2/out/df_first")
    //dfArr(crd_files.length-1).toJavaRDD.coalesce(1).saveAsTextFile("hdfs:///user/ppr.gp2/out/df_last")
    //df.repartition(102)
    //df.persist()

    /*var img = new Image(r.boxDim, spark)
    val avg_str = img.avg_structure(df)
    avg_str.toJavaRDD.coalesce(1).saveAsTextFile("hdfs:///user/ppr.gp2/out/avg")*/


    //df.toJavaRDD.coalesce(1).saveAsTextFile("hdfs:///user/ppr.gp2/out/df-5000_frames")
    //df.persist()
/*
    //Auto image & SuperImposition & Avg Structure starts
    var image = new Image(r.boxDim, spark)

    for (i <- 0 to crd_files.length - 1)
    {
      var temp = r.read_prm(prm_file, arr(0), arr(1), crd_files(i).toString, out_dir, (i.toInt * 10)+1, spark)
      //temp = mask.stripWater(temp)
      dfArr(i) = temp
      dfArr(i).persist()
    }

    var df1 = image.autoImage(dfArr(0),1, r.firstMolCount)
    for (j <- 2 to 10 )
    {
      var temp = image.autoImage(dfArr(0),j, r.firstMolCount)
      df1 = df1.union(temp)
    }
    for (i <- 1 to dfArr.length - 1)
    {
      var currFrame = dfArr(i)
      for (j <- 1 to 10 )
      {
        var frame:Int = i*10 + j
        var temp = image.autoImage(currFrame,frame, r.firstMolCount)
        df1 = df1.union(temp)
      }
    }
    df1 = mask.stripWater(df1)
    df1.toJavaRDD.coalesce(1).saveAsTextFile("hdfs:///user/ppr.gp2/out/autoImage1")
    image.super_impose(df1, spark)
    var avg = image.avg_structure(df1)
    avg.toJavaRDD.coalesce(1).saveAsTextFile("hdfs:///user/ppr.gp2/out/avg_str")

    //Auto Image & SuperImposition & Avg Structure ends
*/
    //strip by water starts
/*
    var df = r.read_prm(prm_file, arr(0), arr(1), crd_files(0), out_dir, 1, spark)
    for (i <- 1 to crd_files.length - 1)
    {
          var temp = r.read_prm(prm_file, arr(0), arr(1), crd_files(i).toString, out_dir, (i.toInt * 10)+1, spark)
          df = df.union(temp)
          df.persist()
    }
    df = mask.stripWater(df)
    df.toJavaRDD.coalesce(1).saveAsTextFile("hdfs:///user/ppr.gp2/out/stripWater")

    //strip by water ends
*/
/*
    //Distance Starts here
    var distance = new Distance()
    for (i <- 0 to crd_files.length - 1)
    {
      var temp = r.read_prm(prm_file, arr(0), arr(1), crd_files(i).toString, out_dir, (i.toInt * 10)+1, spark)
      //temp = mask.stripWater(temp)
      dfArr(i) = temp
      dfArr(i).persist()
    }

    var dist1 = distance.find_dist(1,11,dfArr(0).where($"frame_no" === 1))
    var dist2 = distance.find_dist(1,13000,dfArr(0).where($"frame_no" === 1))

    var strArr = new Array[String](2)

    strArr(0) = "Distance ( 1 , 11 ) = "+dist1
    strArr(1) = "Distance ( 1 , 13000 ) = "+dist2

    sc.parallelize(strArr).coalesce(1).saveAsTextFile("hdfs:///user/ppr.gp2/out/distance")

    //Distance Ends
*/
    //Angle starts
/*    var angle = new Angle()
    var temp = r.read_prm(prm_file, arr(0), arr(1), crd_files(0).toString, out_dir, 1, spark)
    var angle1 = angle.findAngle(temp.where($"frame_no"===1),1,2,7)
    var strAngle = new Array[String](1)
    strAngle(0) = "Angle" + angle1
    sc.parallelize(strAngle).coalesce(1).saveAsTextFile("hdfs:///user/ppr.gp2/out/angle")
    //Angle ends
*/
  }
}