
import calculateKPIofOpendata.ss
import org.apache.spark.sql.SparkSession
import org.scalatest
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.flatspec.AnyFlatSpec

trait SparkSessionProvider {
  ss = SparkSession.builder()
    .master("local[*]")
    .getOrCreate()
}
class CalculKPITest  extends AnyFlatSpec with SparkSessionProvider {

  it should("instanciate a spark session") in {
    var env: Boolean = true
    val ss =  calculateKPIofOpendata.spark_session(env)
  }

}
