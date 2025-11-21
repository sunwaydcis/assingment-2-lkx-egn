import scala.util.{Try, Success, Failure}
import scala.io.{Source, Codec}
import scala.util.{Try, Success, Failure}

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

}