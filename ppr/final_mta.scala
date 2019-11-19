package api
import java.io.{File, PrintWriter}

import org.ejml.simple.{SimpleMatrix, SimpleSVD}

import scala.io.Source
import scala.util.control.Breaks.{break, breakable}
import org.apache.commons.lang3.StringUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import ucar.nc2.NetcdfFile
import org.apache.spark.sql.functions._
import java.util

import scala.math
import org.ejml.simple


object final_mta {
  def mapper(line: String): PDB = {
    val fields = line.split(',')

    val pdb: PDB = PDB(fields(0).toInt, fields(1).toInt, fields(2), fields(3), fields(4).toInt, fields(5).toDouble, fields(6).toDouble, fields(7).toDouble)
    return pdb
  }

  def avg_structure(df: Dataset[PDB], spark: SparkSession): Dataset[Row] = {
    import spark.implicits._

    return df.groupBy($"index").agg(avg($"X"), avg($"Y"), avg($"Z")).sort($"index")
  }

  def getListOfFiles1(dir: String):List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
  }

  def mask(name: String, sampleDF: Dataset[PDB]): Dataset[PDB] = {
    //import spark.implicits._
    val type_mask: Char = name.charAt(0);
    var masking_col: String = "";
    var names = name.slice(1, name.length).split(",")
    if (type_mask == '@') {
      masking_col = "atom"
      //return sampleDF.where($"atom".isin(names:_* ))
    }
    else if (type_mask == '/') {
      masking_col = "res_label";
      //return sampleDF.where($"res_label".isin(names:_* ))
    }
    else {
      printf("Incorrect Input format")
      return sampleDF
    }
    //sampleDF.where( )
    //return sampleDF.where(masking_col+"= \""++"\"")
    return sampleDF.filter(sampleDF(masking_col).isin(names: _*))
  }

  def mask_frame(start: Int, end: Int, intervall: Int, sampleDF: Dataset[PDB]): Dataset[PDB] = {
    // var li :List[Int] = List.range(start,end,intervall)
    //return sampleDF.filter(sampleDF("frame_no").isin(li:_*))
    return sampleDF.filter(sampleDF("frame_no") >= start && sampleDF("frame_no") <= end && (sampleDF("frame_no") - start) % intervall === 0)
  }

  def main(args: Array[String]): Unit = {
    //System.setProperty("hadoop.home.dir", "C:\\winutils")
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("MTA").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder.getOrCreate()
    //val sc = spark.sparkContext
    var r = new Read1()
    val crd_file = args(1)
    var crd_files = getListOfFiles1(crd_file).sorted
    val prm_top_file = args(0)

    val out_dir = args(2)

    val out_file = new java.io.File(out_dir)
    if (!out_file.isFile) {
      out_file.mkdir()
      println(out_dir + " created")
    }
    var time = System.currentTimeMillis()
    var arr = r.read_pointers(prm_top_file, spark)
    var dfArr = new Array[Dataset[PDB]](crd_files.length)
    for (i <- 0 to crd_files.length - 1)
    {
      var temp = r.read_prm(prm_top_file, arr(0), arr(1), crd_files(i).toString, out_dir, (i.toInt * 10)+1, spark)
      dfArr(i) = temp
      dfArr(i).persist()
    }
    rmsdUtil(spark,dfArr(0))
    println("Time taken : " + (System.currentTimeMillis() - time).toString)
  }



  def rmsdUtil(spark:SparkSession,ds : Dataset[PDB]): Unit = {
    val sc = spark.sparkContext

    import spark.implicits._

    val df1 = ds.where($"res_count" <= 1 && $"frame_no" === 1).select($"X", $"Y", $"Z")
    val df2 = ds.where($"res_count" <= 1 && $"frame_no" === 2).select($"X", $"Y", $"Z")
    // df1.show(5)
    // df2.show(5)
    var P = matrix(df1, spark)
    var Q = matrix(df2, spark)
    var kb = new Kabsch(P, Q)
    var Pcentroid = kb.getCentroids.get("P")
    var Ccentroid = kb.getCentroids.get("Q")
    println(Pcentroid)
    var result = kb.tranl(P,Q,Pcentroid,Ccentroid)
    P = result.get("P")
    Q = result.get("Q")
   // print(P)
   // println(Q)
    kb = new Kabsch(P,Q)
    kb.calculate()
    val R = kb.rotation
    P = P.mult(R)

    println(rmsd(P,Q))

  }


  def rmsd(V : SimpleMatrix , W :SimpleMatrix): Double = {
    val D = V.numCols()
    val N = V.getNumElements
    var result = 0.0
    for(i <- 0 to V.numRows()-1)
    {
      var temp:Double = 0
      for(j <- 0 to 2)
      {
        result += (W.get(i,j) - V.get(i,j)) * (W.get(i,j) - V.get(i,j))
      }
    }

    math.sqrt(result/N)
  }

  def matrix(df:Dataset[Row],spark: SparkSession):SimpleMatrix = {
    var arr= df.collect()
    var mat = new SimpleMatrix(arr.length,3)
    //var mat = Array.ofDim[Double](arr.length, 3)
    for(i<- 0 to arr.length-1){

      mat.set(i,0,arr(i)(0).toString.toDouble)
      mat.set(i,1,arr(i)(1).toString.toDouble)
      mat.set(i,2,arr(i)(2).toString.toDouble)
      //mat.get(i,1)=arr(i)(1).toString.toDouble
      //mat.get(i,2)=arr(i)(2).toString.toDouble

    }
    mat
  }




  def gen_pdb(frame_no:Int, df:Array[PDB],total_atoms: Int, at_per_mol:Array[Int], out_dir:String):Unit = {

    var cnt = 0
    var ter_index = 0

    val pw = new PrintWriter(out_dir+"out" + frame_no + ".pdb")
    for (i <- 0 to total_atoms-1) {
      //val x = StringUtils.center(atoms(i).trim(), 4)
      var str = "%-4s %6d %s %3s %5d     %7.3f %7.3f %7.3f".format("ATOM", df(i).index, StringUtils.center(df(i).atom, 4), df(i).res_label, df(i).res_count, df(i).X, df(i).Y, df(i).Z)
      pw.println(str)
      if(i == at_per_mol(ter_index)-1)
      {
        str = "%-4s %6d %s %3s %5d".format("TER", df(i).index+1, "    ", df(i).res_label, df(i).res_count)
        pw.println(str)
        ter_index += 1
      }
    }
    pw.close()
    println(out_dir+"out" +frame_no+".pdb created")
  }
}