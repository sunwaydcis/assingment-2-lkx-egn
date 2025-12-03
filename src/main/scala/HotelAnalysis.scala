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
                         hotelName: String,
                         bookingPrice: Double,
                         discount: Double,
                         override val profitMargin: Double,
                         visitors: Int
                       ) extends Transaction:

  def calculatedProfit: Double = bookingPrice * profitMargin

  override def id: String = bookingId
  override def price: Double = bookingPrice
end HotelBooking

// Generic Analytics Logic
object AnalyticsEngine:

  def findMaxBy[T](data: List[T])(selector: T => Double): T =
    data.maxBy(selector)

  def findMinBy[T](data: List[T])(selector: T => Double): T =
    data.minBy(selector)

  def getRange[T](data: List[T])(selector: T => Double): (Double, Double) =
    val values = data.view.map(selector)
    (values.min, values.max)

  def findMostFrequentCategory[T](data: List[T])(categoryExtractor: T => String): (String, Int) =
    val grouped = data.groupBy(categoryExtractor)
    grouped.map { case (key, items) => (key, items.size) }.maxBy(_._2)

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
        if (cols.length < 24) throw new Exception("Incomplete row")

        val price = cols(20).toDouble
        val discountStr = cols(21).replace("%", "")
        val discount = if (cols(21).contains("%")) discountStr.toDouble / 100 else discountStr.toDouble
        val margin = cols(23).toDouble

        HotelBooking(
          bookingId = cols(0),
          customerOrigin = cols(6),
          destinationCountry = cols(9),
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

// Main Analysis Execution
object HotelAnalysis:

  def main(args: Array[String]): Unit = {
    println("--- Hotel Booking Data Analysis ---")

    val data = DataLoader.loadBookingData("Hotel_Dataset.csv")

    if (data.isEmpty) {
      println("No valid data found. Exiting.")
    } else {
      runAnalysis(data)
    }
  }

  def runAnalysis(data: List[HotelBooking]): Unit = {

    // Question 1
    val (country, count) = AnalyticsEngine.findMostFrequentCategory(data)(_.destinationCountry)
    println(s"1. Country with highest number of bookings: $country with $count bookings.")

    println("\n2. Most Economical Hotels based on criteria:")

    // Question 2
    val (minPrice, maxPrice) = AnalyticsEngine.getRange(data)(_.bookingPrice)
    val (minDisc, maxDisc)   = AnalyticsEngine.getRange(data)(_.discount)
    val (minMarg, maxMarg)   = AnalyticsEngine.getRange(data)(_.profitMargin)

    println(s"   [Stats] Price Range:    SGD $minPrice to SGD $maxPrice")
    println(s"   [Stats] Discount Range: ${(minDisc * 100).toInt}% to ${(maxDisc * 100).toInt}%")
    println(s"   [Stats] Margin Range:   $minMarg to $maxMarg")

    def normalize(value: Double, min: Double, max: Double): Double =
      if (max == min) 0.0 else (value - min) / (max - min)

    val mostEconomical = AnalyticsEngine.findMinBy(data) { b =>
      val normPrice = normalize(b.bookingPrice, minPrice, maxPrice)
      val normDisc  = normalize(b.discount, minDisc, maxDisc)
      val normMarg  = normalize(b.profitMargin, minMarg, maxMarg)

      normPrice + (1.0 - normDisc) + normMarg
    }

    println(s"\n   >> Final Answer: The most economical option is ${mostEconomical.hotelName}")
    println(s"      (Price: ${mostEconomical.bookingPrice}, Discount: ${mostEconomical.discount}, Margin: ${mostEconomical.profitMargin})")

    // Question 3
    val hotelStats = data.groupBy(_.hotelName).view.map { case (hotel, bookings) =>
      val totalProfit = bookings.map(_.calculatedProfit).sum
      val totalVisitors = bookings.map(_.visitors).sum
      (hotel, totalVisitors, totalProfit)
    }

    val mostProfitable = hotelStats.maxBy(_._3)

    println(s"\n3. Most Profitable Hotel: ${mostProfitable._1}")
    println(s"   Total Visitors: ${mostProfitable._2}")
    println(s"   Total Profit Generated: SGD ${f"${mostProfitable._3}%.2f"}")
  }

end HotelAnalysis