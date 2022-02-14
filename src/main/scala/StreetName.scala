import scala.util.control.Breaks.{break, breakable}

/***
 *
 *
 *
 */
class StreetName{
  private var listOfBasicStreetName=Array("BOULEVARD", "AVENUE", "ALLEE", "ALLEES", "PLACE", "QUAI", "COURS", "RUE","PARVIS", "TOUR","BD", "AV")
  private var street : String=""

  def this(address:String){
    this()
    this.street=address
  }

  def findStreetName(): String = {
    var streetWords = street.split("\\s")
    //var streetWordsdrop:Any
    breakable {
      for(i<-0 to streetWords.length-1){
        if(listOfBasicStreetName.contains(streetWords(i).toUpperCase)){
          streetWords=streetWords.drop(i)
          break()
        }
      }
    }
    val result=streetWords.mkString(" ")
    return result
  }

}
