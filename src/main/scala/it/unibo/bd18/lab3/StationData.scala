package it.unibo.bd18.lab3

case class StationData(
                        usaf: String,
                        wban: String,
                        name: String,
                        country: String,
                        state: String,
                        call: String,
                        latitude: Double,
                        longitude: Double,
                        elevation: Double,
                        dateBegin: String,
                        dateEnd: String
                      )

object StationData {

  def apply(row: String): StationData = extract(row)

  def extract(row: String): StationData = {
    def getDouble(str: String): Double = {
      if (str.isEmpty)
        return 0
      else
        return str.toDouble
    }

    val columns = row.split(",").map(_.replaceAll("\"", ""))
    val latitude = getDouble(columns(6))
    val longitude = getDouble(columns(7))
    val elevation = getDouble(columns(8))
    StationData(columns(0), columns(1), columns(2), columns(3), columns(4), columns(5), latitude, longitude, elevation, columns(9), columns(10))
  }

}
