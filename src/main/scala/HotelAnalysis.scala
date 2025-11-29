import scala.util.{Try, Success, Failure}
import scala.io.{Source, Codec}
import scala.util.{Try, Success, Failure}
import java.nio.charset.CodingErrorAction

//Define the Data Model
case class Booking(
                    bookingId: String,
                    customerOrigin: String,
                    destinationCountry: String,
                    hotelName: String,
                    bookingPrice: Double,
                    discount: Double,
                    profitMargin: Double,
                    visitors: Int
                  )

object HotelAnalysis {

  //File Reading logic
  def loadBookingData(filePath: String): List[Booking] = {
    try {
      implicit val codec: Codec = Codec("UTF-8")
      codec.onMalformedInput(CodingErrorAction.REPLACE)
      codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

      val source = Source.fromFile(filePath)
      val lines = source.getLines().toList
      source.close()

      lines.drop(1).flatMap { line =>

        val cols = line.split(",").map(_.trim)

        Try {

          if (cols.length < 24) throw new Exception("Incomplete row")
          val price = cols(20).toDouble
          val discountStr = cols(21).replace("%", "")
          val discount = if (cols(21).contains("%")) discountStr.toDouble / 100 else discountStr.toDouble
          val margin = cols(23).toDouble

          Booking(
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
      }
    } catch {
      case e: Exception =>
        println(s"Error reading file: ${e.getMessage}")
        e.printStackTrace()
        List.empty[Booking]
    }
  }

  def main(args: Array[String]): Unit = {
    println("--- Hotel Booking Data Analysis ---")

    val data = loadBookingData("Hotel_Dataset.csv")

    if (data.isEmpty) {
      println("No data found. Please check the dataset file.")
    } else {

      // ------------------ Question 1 ------------------ //
      // Which country has the highest number of bookings //
      val bookingsByCountry = data.groupBy(_.destinationCountry)
      val topCountry = bookingsByCountry.map { case (country, bookings) =>
        (country, bookings.size)
      }.maxBy(_._2)

      // ----------------------- Question 2 ------------------------- //
      // Which hotel offers the most economical option for customers? //

      println(s"1. Country with highest number of bookings: ${topCountry._1} with ${topCountry._2} bookings.")

      // a. Best Price
      val cheapestHotel = data.minBy(_.bookingPrice)

      // b. Best Discount
      val highestDiscountHotel = data.maxBy(_.discount)

      // c. Best Profit Margin
      val lowestMarginHotel = data.minBy(_.profitMargin)

      println("\n2. Most Economical Hotels based on criteria:")
      println(s"   a. Booking Price: ${cheapestHotel.hotelName} (SGD ${cheapestHotel.bookingPrice})")
      println(s"   b. Discount: ${highestDiscountHotel.hotelName} (${(highestDiscountHotel.discount * 100).toInt}%)")
      println(s"   c. Profit Margin (Lowest): ${lowestMarginHotel.hotelName} (${lowestMarginHotel.profitMargin})")

    }
  }
}