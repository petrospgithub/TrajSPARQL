package stOperators

object Contains {
  def apply(a_minx:Double, a_maxx:Double, a_miny:Double, a_maxy:Double, a_mint:Long, a_maxt:Long,
            b_x:Double, b_y:Double, b_t:Long): Boolean = {

    a_mint<=b_t && b_t<=a_maxt &&
      a_minx<=b_x && b_x<=a_maxx &&
      a_miny<=b_y && b_y<=a_maxy
  }

  def apply(a_minx:Double, a_maxx:Double, a_miny:Double, a_maxy:Double, a_mint:Long, a_maxt:Long,
            b_minx:Double, b_maxx:Double, b_miny:Double, b_maxy:Double, b_mint:Long, b_maxt:Long): Boolean = {

    a_minx <= b_minx && b_maxx<= a_maxx &&
      a_miny <= b_miny && b_maxy <= a_maxy &&
      a_mint <= b_mint && b_maxt <= a_maxt
  }
}
