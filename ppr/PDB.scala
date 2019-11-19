package api
case class PDB(frame_no:Int, index:Int, atom:String, res_label:String, res_count:Int,X:Double, Y:Double,Z:Double)

case class atom_coordinates(x:Double,y:Double,z:Double)