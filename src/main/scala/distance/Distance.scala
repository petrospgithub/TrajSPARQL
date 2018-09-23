package distance

object Distance {
  def getHaversineDistance (lat1:Double, lon1:Double, lat2:Double, lon2:Double):Double={
    val deltaLat = Math.toRadians(lat2 - lat1)
    val deltaLon = Math.toRadians(lon2 - lon1)
    val a = Math.pow(Math.sin(deltaLat / 2.0D), 2) + Math.cos(Math.toRadians(lat2)) * Math.cos(Math.toRadians(lat1)) * Math.pow(Math.sin(deltaLon / 2.0D), 2)
    val greatCircleDistance = 2.0D * Math.atan2(Math.sqrt(a), Math.sqrt(1.0D - a))
    //3958.761D * greatCircleDistance           //Return value in miles
    //3440.0D * greatCircleDistance             //Return value in nautical miles
    6371000.0D * greatCircleDistance            //Return value in meters, assuming Earth radius is 6371 km
  }

  def getEuclideanDistance (lat1:Double, lon1:Double, lat2:Double, lon2:Double):Double={
    val xcoord = Math.abs (lon1 - lon2)
    val ycoord = Math.abs (lat1- lat2)

    Math.sqrt(xcoord*xcoord +ycoord*ycoord)
  }

  def getManhattanDistance (lat1:Double, lon1:Double, lat2:Double, lon2:Double):Double={
    val xcoord = Math.abs (lon1 - lon2)
    val ycoord = Math.abs (lat1- lat2)
    xcoord+ycoord
  }

  def timeElasped(t1:Long, t2:Long): Unit ={
    Math.abs(t1-t2)
  }
}
