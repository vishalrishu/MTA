package api
import java.util
import java.util.HashMap
import org.ejml.data.Matrix
import org.ejml.simple.{SimpleMatrix, SimpleSVD}



class Kabsch @throws[IllegalArgumentException]
/**
  * Data set P
  */
(var P: SimpleMatrix,

 /**
  * Data set Q
  */
 var Q: SimpleMatrix){
  if (!isValidInputMatrix(P) || !isValidInputMatrix(Q)) throw new IllegalArgumentException
  if (P.numCols != Q.numCols || P.numRows != Q.numRows) throw new IllegalArgumentException
  calculate()
  /**
   * Rotation matrix
   */
  var rotation: SimpleMatrix = null
  /**
   * Translation matrix
   */
  //var translation: SimpleMatrix = null

  /**
   * Check if the given matrix is a valid matrix for the Kabsch algorithm.
   *
   * @param     M
   * The matrix to be checked.
   * @return True if and only if the number of columns is at least two
   *         and the number of rows is at least the number of columns.
   */
  private def isValidInputMatrix(M: SimpleMatrix) = M.numCols >= 2 && M.numRows >= M.numCols

  /**
   * Calculate the covariance matrix for the matrices P and Q.
   *
   * @return The covariance matrix
   */
  private def getCovariance = {
    val centroids = getCentroids
    val Pcentroid = centroids.get("P")
    val Qcentroid = centroids.get("Q")
    val n = P.numCols
    val m = P.numRows
    var A = new SimpleMatrix(Array.ofDim[Double](n, n))
    for(j:Int <- 0 to m-1) {
      val Pj = new SimpleMatrix(1, n)
      val Qj = new SimpleMatrix(1, n)
      for(i:Int <- 0 to n-1) {
        Qj.set(0, i, Q.get(j, i) - Qcentroid.get(0, i))
        Pj.set(0, i, P.get(j, i) - Pcentroid.get(0, i))
      }
      val S = Qj.transpose.mult(Pj)
      A = A.plus(S)
    }
    A
  }

  /**
   * Compute the Singular Value Decomposition of the Covariance Matrix. Use the
   * results to create the rotation and translation matrix.
   */
  def calculate(): Unit = { // Calculate Singular Value Decomposition of covariance matrix

    val A = getCovariance
    //print("A mAtrix")
   // println(A)
    val svd = new SimpleSVD(A.getMatrix, true)
    //A.getM
    //val svd = new SimpleSVD[SimpleMatrix](A.getMatrix, true)
    //svd = svd.
    val UT = svd.getU.asInstanceOf[SimpleMatrix].transpose()
   // println("UT")
   // print(UT)
    println()
    val V = svd.getV.asInstanceOf[SimpleMatrix]
    // Decide whether we need to correct our rotation matrix to insure a right-handed coordinate system
    val R = V.mult(UT)
    val d = Math.signum(R.determinant).toInt
    // Calculate the optimal rotation matrix
    val dm = SimpleMatrix.identity(P.numCols)
    dm.set(P.numCols - 1, P.numCols - 1, d)
    this.rotation = V.mult(dm).mult(UT)
    // Calculate the translation matrix
    //val centroids = getCentroids
    // val Pcentroid = centroids("P","Q")
   // val Pcentroid = centroids.get("P")
   // val Qcentroid = centroids.get("Q")
   // this.translation = R.mult(Pcentroid.transpose).scale(-1).plus(Qcentroid.transpose)
  }
  def tranl(P: SimpleMatrix, Q: SimpleMatrix,Pcentroid :SimpleMatrix,Ccentroid:SimpleMatrix) :util.HashMap[String,SimpleMatrix] =
  {
    val result = new util.HashMap[String,SimpleMatrix]()
    var A = new SimpleMatrix(Array.ofDim[Double](P.numRows(), P.numCols()))
    var B = new SimpleMatrix(Array.ofDim[Double](P.numRows(), P.numCols()))
    for(i<- 0 to P.numRows()-1){
      for(j<- 0 to P.numCols()-1)
      {
        A.set(i,j,P.get(i,j)-Pcentroid.get(0,j))
        B.set(i,j,Q.get(i,j)-Ccentroid.get(0,j))
      }
    }
    result.put("P",A)
    result.put("Q",B)
    result
  }
  /**
   * Compute the centroids for matrix P and Q
   *
   * @return HashMap containing the centroids for matrix P and Q
   */
   def getCentroids:util.HashMap[String,SimpleMatrix] = {
    val result = new util.HashMap[String,SimpleMatrix]()
    val n = P.numCols
    val m = P.numRows
    val Pcentroid = new SimpleMatrix(1, n)
    val Qcentroid = new SimpleMatrix(1, n)
    for(i:Int <- 0 to n-1) {
      var sumP:Double = 0
      var sumQ:Double = 0
      for(j:Int <- 0 to m-1) {
        sumP += P.get(j, i)
        sumQ += Q.get(j, i)
      }
      Pcentroid.set(0, i, sumP / m)
      Qcentroid.set(0, i, sumQ / m)
    }
    result.put("P", Pcentroid)
    result.put("Q", Qcentroid)
    result
  }

  /**
   * Get the translation vector.
   *
   * @return The translation vector.
   *//*
  def getTranslationVector: SimpleMatrix = this.translation

  /**
   * Get the translation matrix.
   *
   * @return The translation matrix.
   */
  def getTranslationMatrix: SimpleMatrix = {
    val matrix =  Array.ofDim[Double](P.numRows(), 3)
    val transVec = getTranslationVector

    for(i:Int <- 0 to P.numRows()-1) {
      for(j:Int <- 0 to 2) {
        matrix(j)(i) = transVec.get(j, 0)
      }
    }
    new SimpleMatrix(matrix)
  }*/

  /**
   * Get the rotation matrix
   *
   * @return The rotation matrix
   */
  def getRotation: SimpleMatrix = this.rotation
}