import scala.io.{Codec, Source}
import scala.util.{Failure, Success, Try}
import java.nio.charset.CodingErrorAction

// Define Transaction Interface
trait Transaction:
  def id: String
  def price: Double
  def profitMargin: Double

// Define Data Model
case class HotelBooking(
                         bookingId: String,
                         customerOrigin: String,
                         destinationCountry: String,
                         city: String,
                         hotelName: String,
                         bookingPrice: Double,
                         discount: Double,
                         override val profitMargin: Double,
                         visitors: Int
                       ) extends Transaction:

  override def id: String = bookingId
  override def price: Double = bookingPrice
end HotelBooking

// Class to hold Grouped Data Stats
case class HotelGroupStats(
                            country: String,
                            hotelName: String,
                            city: String,
                            avgPrice: Double,
                            avgDiscount: Double,
                            avgMargin: Double,
                            totalVisitors: Int
                          )

// Generic Analytics Logic
object AnalyticsEngine:

  def findMaxBy[T](data: List[T])(selector: T => Double): T =
    data.maxBy(selector)

  def findMinBy[T](data: List[T])(selector: T => Double): T =
    data.minBy(selector)

  // Generic method to find Min and Max of a value set
  def getRange[T](data: List[T])(selector: T => Double): (Double, Double) =
    val values = data.view.map(selector)
    (values.min, values.max)

  def findMostFrequentCategory[T](data: List[T])(categoryExtractor: T => String): (String, Int) =
    val grouped = data.groupBy(categoryExtractor)
    grouped.map { case (key, items) => (key, items.size) }.maxBy(_._2)

  // Normalization Logic
  def normalizeInverted(value: Double, min: Double, max: Double): Double =
    if (max == min) 100.0
    else (1.0 - ((value - min) / (max - min))) * 100.0

  def normalizeStandard(value: Double, min: Double, max: Double): Double =
    if (max == min) 0.0
    else ((value - min) / (max - min)) * 100.0

end AnalyticsEngine

// File Reading Logic
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
        println(s"CRITICAL ERROR: Could not read file. ${e.getMessage}")
        List.empty[HotelBooking]
    }
  }

  private def parseLines(lines: List[String]): List[HotelBooking] = {
    lines.view.drop(1).flatMap { line =>
      val cols = line.split(",").map(_.trim)

      Try {
        // Validation check for column count
        if (cols.length < 24) throw new Exception("Incomplete row")

        val price = cols(20).toDouble
        val discountStr = cols(21).replace("%", "")
        val discount = if (cols(21).contains("%")) discountStr.toDouble / 100 else discountStr.toDouble
        val margin = cols(23).toDouble

        // Assumption: City is at index 10. Update this index if your CSV differs.
        val cityStr = if (cols.length > 10) cols(10) else "Unknown"

        HotelBooking(
          bookingId = cols(0),
          customerOrigin = cols(6),
          destinationCountry = cols(9),
          city = cityStr,
          hotelName = cols(16),
          bookingPrice = price, // Taking price "as is"
          discount = discount,
          profitMargin = margin,
          visitors = cols(11).toInt
        )
      }.toOption
    }.toList
  }

end DataLoader

// Main Analysis Execution
object HotelAnalysis:

  def main(args: Array[String]): Unit = {
    println("=-=-=-=-=-=-=-=- Hotel Booking Data Analysis -=-=-=-=-=-=-=-=-=")

    val data = DataLoader.loadBookingData("Hotel_Dataset.csv")

    if (data.isEmpty) {
      println("No valid data found. Exiting.")
    } else {
      runAnalysis(data)
    }
  }

  def runAnalysis(data: List[HotelBooking]): Unit = {

    // Group Data by (Country, Hotel, City)
    val groupedData = data.groupBy(b => (b.destinationCountry, b.hotelName, b.city))
      .map { case ((country, hotel, city), bookings) =>
        val avgPrice = bookings.map(_.bookingPrice).sum / bookings.size
        val avgDiscount = bookings.map(_.discount).sum / bookings.size
        val avgMargin = bookings.map(_.profitMargin).sum / bookings.size
        val totalVis = bookings.map(_.visitors).sum

        HotelGroupStats(country, hotel, city, avgPrice, avgDiscount, avgMargin, totalVis)
      }.toList

    // Question 1
    val (country, count) = AnalyticsEngine.findMostFrequentCategory(data)(_.destinationCountry)
    println(s"1. Country with highest number of bookings: $country with $count bookings.")


    // --- Question 2
    println("\n2. Most Economical Hotel (Based on Score Logic):")

    val (minP, maxP) = AnalyticsEngine.getRange(groupedData)(_.avgPrice)
    val (minD, maxD) = AnalyticsEngine.getRange(groupedData)(_.avgDiscount)
    val (minM, maxM) = AnalyticsEngine.getRange(groupedData)(_.avgMargin)

    val bestEconomical = groupedData.map { group =>

      val scorePrice = AnalyticsEngine.normalizeInverted(group.avgPrice, minP, maxP)

      val scoreDiscount = AnalyticsEngine.normalizeStandard(group.avgDiscount, minD, maxD)

      val scoreMargin = AnalyticsEngine.normalizeInverted(group.avgMargin, minM, maxM)

      val finalScore = (scorePrice + scoreDiscount + scoreMargin) / 3.0

      (group, finalScore, scorePrice, scoreDiscount, scoreMargin)
    }.maxBy(_._2)

    val (ecoHotel, ecoScore, sP, sD, sM) = bestEconomical

    println(s"   Winner: ${ecoHotel.hotelName} (${ecoHotel.city}, ${ecoHotel.country})")
    println(s"   Final Economical Score: ${f"$ecoScore%.2f"}")
    println(s"   Details -> Avg Price: ${f"${ecoHotel.avgPrice}%.2f"} (Score: ${f"$sP%.1f"}), " +
      s"Avg Discount: ${f"${ecoHotel.avgDiscount * 100}%.1f"}%% (Score: ${f"$sD%.1f"}), " +
      s"Avg Margin: ${f"${ecoHotel.avgMargin}%.4f"} (Score: ${f"$sM%.1f"})")


    // --- Question 3
    println("\n3. Most Profitable Hotel (Based on Visitor & Margin Score):")

    val (minVis, maxVis) = AnalyticsEngine.getRange(groupedData)(_.totalVisitors.toDouble)

    val bestProfitable = groupedData.map { group =>

      val scoreVis = AnalyticsEngine.normalizeStandard(group.totalVisitors.toDouble, minVis, maxVis)


      val scoreMarg = AnalyticsEngine.normalizeStandard(group.avgMargin, minM, maxM)

      val finalScore = (scoreVis + scoreMarg) / 2.0

      (group, finalScore, scoreVis, scoreMarg)
    }.maxBy(_._2)

    val (profHotel, profScore, sV, sMarg) = bestProfitable

    println(s"   Winner: ${profHotel.hotelName} (${profHotel.city}, ${profHotel.country})")
    println(s"   Final Profitable Score: ${f"$profScore%.2f"}")
    println(s"   Details -> Total Visitors: ${profHotel.totalVisitors} (Score: ${f"$sV%.1f"}), " +
      s"Avg Margin: ${f"${profHotel.avgMargin}%.4f"} (Score: ${f"$sMarg%.1f"})")
  }

end HotelAnalysis