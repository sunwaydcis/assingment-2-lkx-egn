import scala.io.{Codec, Source}
import scala.util.{Failure, Success, Try}
import java.nio.charset.CodingErrorAction

trait Transaction:
  def id: String
  def amount: Double
end Transaction

// ------ Data model
case class HotelBooking(
                         bookingId: String,
                         customerOrigin: String,
                         destinationCountry: String,
                         city: String,
                         hotelName: String,
                         bookingPrice: Double,
                         discount: Double,
                         profitMargin: Double,
                         visitors: Int
                       ) extends Transaction:

  override def id: String = bookingId
  override def amount: Double = bookingPrice
end HotelBooking

case class GroupStats(
                       country: String,
                       hotelName: String,
                       city: String,
                       avgPrice: Double,
                       avgDiscount: Double,
                       avgMargin: Double,
                       totalVisitors: Int
                     )

// ------ Analytics
object AnalyticsEngine:

  // find the maximum element based on a numeric selector
  def findMaxBy[T](data: List[T])(selector: T => Double): T =
    data.maxBy(selector)

  // find the Range (Min, Max)
  def getRange[T](data: List[T])(selector: T => Double): (Double, Double) =
    val values = data.view.map(selector) // .view optimizes performance (lazy evaluation)
    (values.min, values.max)

  // find frequency
  def findMostFrequentCategory[T](data: List[T])(categoryExtractor: T => String): (String, Int) =
    data.groupBy(categoryExtractor)
      .map { case (key, items) => (key, items.size) }
      .maxBy(_._2)

  def calculateScore(value: Double, min: Double, max: Double, higherIsBetter: Boolean): Double =
    if (max == min) return 50.0 // Avoid division by zero

    val normalized = ((value - min) / (max - min)) * 100.0

    if (higherIsBetter) normalized
    else 100.0 - normalized

end AnalyticsEngine

// ---- Data Loading
object DataLoader:

  def loadBookingData(filePath: String): List[HotelBooking] = {
    implicit val codec: Codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    Try {
      val source = Source.fromFile(filePath)
      val lines = source.getLines().toList
      source.close()
      lines
    } match {
      case Success(lines) => parseLines(lines)
      case Failure(e) =>
        println(s"Error: Could not read file. ${e.getMessage}")
        List.empty[HotelBooking]
    }
  }

  private def parseLines(lines: List[String]): List[HotelBooking] = {
    lines.view.drop(1).flatMap { line =>
      val cols = line.split(",").map(_.trim)

      Try {

        if (cols.length < 24) throw new Exception("Incomplete row")

        val price = cols(20).toDouble
        val discountStr = cols(21).replace("%", "")
        val discount = if (cols(21).contains("%")) discountStr.toDouble / 100 else discountStr.toDouble
        val margin = cols(23).toDouble
        val cityStr = if (cols.length > 10) cols(10) else "Unknown"

        HotelBooking(
          bookingId = cols(0),
          customerOrigin = cols(6),
          destinationCountry = cols(9),
          city = cityStr,
          hotelName = cols(16),
          bookingPrice = price,
          discount = discount,
          profitMargin = margin,
          visitors = cols(11).toInt
        )
      }.toOption
    }.toList
  }

end DataLoader

object HotelAnalysis:

  def main(args: Array[String]): Unit = {
    println("--- Hotel Booking Data Analysis ---")

    val data = DataLoader.loadBookingData("Hotel_Dataset.csv")

    if (data.isEmpty)
      println("No data available.")
    else
      performAnalysis(data)
  }

  def performAnalysis(data: List[HotelBooking]): Unit = {

    val groupedStats = data.groupBy(b => (b.destinationCountry, b.hotelName, b.city))
      .map { case ((country, hotel, city), bookings) =>
        GroupStats(
          country, hotel, city,
          avgPrice = bookings.map(_.bookingPrice).sum / bookings.size,
          avgDiscount = bookings.map(_.discount).sum / bookings.size,
          avgMargin = bookings.map(_.profitMargin).sum / bookings.size,
          totalVisitors = bookings.map(_.visitors).sum
        )
      }.toList

    // --- Question 1:
    val (country, count) = AnalyticsEngine.findMostFrequentCategory(data)(_.destinationCountry)
    println(s"1. Country with highest number of bookings: $country with $count bookings.")

    // --- Question 2:

    val (minP, maxP) = AnalyticsEngine.getRange(groupedStats)(_.avgPrice)
    val (minD, maxD) = AnalyticsEngine.getRange(groupedStats)(_.avgDiscount)
    val (minM, maxM) = AnalyticsEngine.getRange(groupedStats)(_.avgMargin)

    val bestEconomical = groupedStats.map { g =>

      val sPrice = AnalyticsEngine.calculateScore(g.avgPrice, minP, maxP, higherIsBetter = false)

      val sDisc = AnalyticsEngine.calculateScore(g.avgDiscount, minD, maxD, higherIsBetter = true)

      val sMarg = AnalyticsEngine.calculateScore(g.avgMargin, minM, maxM, higherIsBetter = false)

      val avgScore = (sPrice + sDisc + sMarg) / 3.0
      (g, avgScore, sPrice, sDisc, sMarg)
    }.maxBy(_._2)

    val (ecoHotel, ecoScore, ecoPriceScore, ecoDiscScore, ecoMargScore) = bestEconomical

    println(s"\n2. Most Economical Hotel (Based on Score Logic):")
    println(s"   Winner: ${ecoHotel.hotelName} (${ecoHotel.city}, ${ecoHotel.country})")
    println(s"   Final Economical Score: ${f"$ecoScore%.2f"}")
    println(s"   Details -> Avg Price: ${f"${ecoHotel.avgPrice}%.2f"} (Score: ${f"$ecoPriceScore%.1f"}), " +
      s"Avg Discount: ${f"${ecoHotel.avgDiscount * 100}%.1f"}%% (Score: ${f"$ecoDiscScore%.1f"}), " +
      s"Avg Margin: ${f"${ecoHotel.avgMargin}%.4f"} (Score: ${f"$ecoMargScore%.1f"})")


    // --- Question 3:

    val (minVis, maxVis) = AnalyticsEngine.getRange(groupedStats)(_.totalVisitors.toDouble)

    val bestProfitable = groupedStats.map { g =>

      val sVis = AnalyticsEngine.calculateScore(g.totalVisitors.toDouble, minVis, maxVis, higherIsBetter = true)

      val sMarg = AnalyticsEngine.calculateScore(g.avgMargin, minM, maxM, higherIsBetter = true)

      val avgScore = (sVis + sMarg) / 2.0
      (g, avgScore, sVis, sMarg)
    }.maxBy(_._2)

    val (profHotel, profScore, profVisScore, profMargScore) = bestProfitable

    println(s"\n3. Most Profitable Hotel (Based on Visitor & Margin Score):")
    println(s"   Winner: ${profHotel.hotelName} (${profHotel.city}, ${profHotel.country})")
    println(s"   Final Profitable Score: ${f"$profScore%.2f"}")
    println(s"   Details -> Total Visitors: ${profHotel.totalVisitors} (Score: ${f"$profVisScore%.1f"}), " +
      s"Avg Margin: ${f"${profHotel.avgMargin}%.4f"} (Score: ${f"$profMargScore%.1f"})")
  }

end HotelAnalysis