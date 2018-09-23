package utils

import types.CellST

object ArraySearch {

  def binarySearchIterative(list: Array[(CellST,Int)], target: Long): Int = {
    var left = 0
    var right = list.length-1
    var mid = -1
    var done=false
    while (left<=right && !done) {

      mid = left + (right-left)/2

      //println(mid)

      if(list(mid)._1.range.mint <= target && target <= list(mid)._1.range.maxt) {
        //println("mpike")
        //return mid
        done=true
      }
      else if ( target < (list(mid)._1.range.mint+list(mid)._1.range.maxt)/2 )
        right = mid-1
      else
        left = mid+1
    }
    //-1
    mid
  }

  def binarySearchIterative(list: Array[CellST], target: Long): Int = {
    var left = 0
    var right = list.length-1

    //println("left: "+left)
    //println("right: "+right)
    var mid= -1
    var done=false

    while (left<=right && !done) {
    //  println("------------------------------------------------------------")

      mid = left + (right-left)/2
    //  println("mid: "+mid)

    //  println("min "+list(mid).range.mint)
    //  println("max "+list(mid).range.maxt)

   //   println("left: "+left)
   //   println("right: "+right)

      if(list(mid).range.mint <= target && target <= list(mid).range.maxt) {
        //return mid
        done=true
      }
      else if ( target < (list(mid).range.mint+list(mid).range.maxt)/2 )
        right = mid-1
      else
        left = mid+1
    }
   // println("------------------------------------------------------------")

    mid
  }

}

