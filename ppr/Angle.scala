package api

import org.apache.spark.sql.Dataset
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import scala.math._
import org.apache.spark.sql.SparkSession
import  org.apache.spark
import org.apache.spark.SparkContext


/*
point_v class container for atom_coordinates and implement (-,*,point_norm) function
 */
class point_v(cor:atom_coordinates) {
  val x:Double =cor.x
  val y:Double = cor.y
  val z:Double = cor.z
  def -(that:point_v) = new  point_v(atom_coordinates(this.x - that.x,this.y - that.y,this.z - that.z))
  def point_norm():Double = math.sqrt(math.pow(this.x,2.0)+math.pow(this.y,2.0)+math.pow(this.z,2.0))
  def * (that:point_v):Double = this.x * that.x + this.y * that.y + this.z * that.z
}
/* Angle class
class for finding Angle in radius and degree take arguments as an array of atom_coordinate of size 3
call as
val angle = new Angle(atom_coordinates) //atom_coordinates is an Array of size 3
 */
class F_Angle(cor:Array[atom_coordinates])
{
  val q = new point_v(cor(0))
  val p = new point_v(cor(1))
  val r = new point_v(cor(2))
  /*
  function to find angle in radian
   */
  def find_Angle():Double ={
    val pq = q - p
    val pr = r - p
    val norm_pq = pq.point_norm()
    val norm_pr = pr.point_norm()
    val Dot_pq_pr = pq * pr
    val Angl = Dot_pq_pr / (norm_pq * norm_pr)
    val Angle = math.acos(Angl)
    Angle
  }
  /*
  function to find angle in degree
   */
  def find_Angle_deg():Double={
    find_Angle()*(180/math.Pi)
  }
}

class Angle {

  def findAngle(df:Dataset[PDB],atom1:Int,atom2:Int,atom3:Int) :Double = {
    val mask = new Mask()
    val at1 = mask.mask_index(atom1,df)
    val at2 = mask.mask_index(atom2,df)
    val at3 = mask.mask_index(atom3,df)
    val At = new Array[atom_coordinates](3)
    At(0) = new atom_coordinates(at1.select("X").collect()(0)(0).toString().toDouble,at1.select("Y").collect()(0)(0).toString().toDouble,at1.select("Z").collect()(0)(0).toString().toDouble)
    At(1)  = new atom_coordinates(at2.select("X").collect()(0)(0).toString().toDouble,at2.select("Y").collect()(0)(0).toString().toDouble,at2.select("Z").collect()(0)(0).toString().toDouble)
    At(2)  = new atom_coordinates(at3.select("X").collect()(0)(0).toString().toDouble,at3.select("Y").collect()(0)(0).toString().toDouble,at3.select("Z").collect()(0)(0).toString().toDouble)
    val angle_r = new F_Angle((At)).find_Angle_deg()
    angle_r
  }
}


