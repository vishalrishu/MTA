package api
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.avg
import api.PDB
import org.apache.spark.SparkContext


class Image(dim: Array[Double],sp: SparkSession) extends java.io.Serializable {
  val boxDim:Array[Double] = Array(123.672, 128.370, 119.579)
  val spark = sp
  import spark.implicits._

  def super_impose(df:Dataset[PDB],spark:SparkSession) :Unit ={
    import spark.implicits._
    val max_X=df.groupBy($"index").max("X")
    val max_Y=df.groupBy($"index").max("Y")
    val max_Z=df.groupBy($"index").max("Z")
    val min_X=df.groupBy($"index").min("X")
    val min_Y=df.groupBy($"index").min("Y")
    val min_Z=df.groupBy($"index").min("Z")

    var maxDF = max_X.join(max_Y,"index").join(max_Z,"index")
    var minDF = min_X.join(min_Y,"index").join(min_Z,"index")

    var finaldf=maxDF.join(minDF,"index").sort("index")
    finaldf.toJavaRDD.coalesce(1).saveAsTextFile("hdfs:///user/ppr.gp2/out/superImpose")
    //finaldf.show(5)
  }

  def mod(len : Double, crd : Double) : Double = {
    var res : Double = 0.0
    if(crd > 0 && crd < len) {
      return crd
    } else if( crd > 0 && crd > len) {
      res = crd % (len.toInt)
      return res
    } else if( crd < 0) {
      res = -1*crd
      if(res < len) {
        return res
      } else {
        return res%(len.toInt)
      }
    }
    return res
  }

  def avg_structure(df:Dataset[PDB]): Dataset[Row] = {
    return df.groupBy($"index").agg(avg($"X").as("X"), avg($"Y").as("Y"), avg($"Z").as("Z")).sort($"index")
  }

  def autoImage(df: Dataset[PDB],frameNo:Int, residuePointer:Int) : Dataset[PDB] = {
    import spark.implicits._

    val df1 = df.where($"index" <= residuePointer && $"frame_no" === frameNo).select(avg($"X"),avg($"Y"), avg($"Z"))
    val avgCrds= df1.collect()(0)

    var boxCenter = new Array[Int](3)
    var diff = new Array[Int](3)
    for(i <- 0 to 2)
    {
      boxCenter(i) = (boxDim(i) / 2).toInt
      diff(i) = (boxCenter(i) - avgCrds(i).toString.toDouble.toInt)
    }
    var df2 = df.map(row => PDB(row.frame_no, row.index, row.atom, row.res_label, row.res_count,(mod(boxDim(0),row.X+diff(0))),mod(boxDim(1),row.Y+diff(1)),mod(boxDim(2),row.Z+diff(2))))

    return df2
  }
}
