package stOperators

object Intersects {
  def apply(a_minx:Double, a_maxx:Double, a_miny:Double, a_maxy:Double, a_mint:Long, a_maxt:Long,
            b_minx:Double, b_maxx:Double, b_miny:Double, b_maxy:Double, b_mint:Long, b_maxt:Long): Boolean = {

    b_maxx >= a_minx && b_minx<= a_maxx &&
      b_maxy >= a_miny && b_miny <= a_maxy &&
      b_maxt >= a_mint && b_mint <= a_maxt
  }

  def apply(a_minx:Double, a_maxx:Double, a_miny:Double, a_maxy:Double, a_mint:Long, a_maxt:Long,
            b_x:Double, b_y:Double, b_t:Long): Boolean = {

    b_x >= a_minx && b_x<= a_maxx &&
      b_y >= a_miny && b_y <= a_maxy &&
      b_t >= a_mint && b_t <= a_maxt
  }
}
