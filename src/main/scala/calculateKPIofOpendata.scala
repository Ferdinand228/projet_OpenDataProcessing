import org.apache.spark.sql._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession, functions}
import org.apache.spark.sql.catalyst.expressions.codegen.FalseLiteral
import org.apache.spark.sql.functions.{array_distinct, col, concat_ws, current_date, exp, expr, lit, not, regexp_replace, udf, when}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

import java.nio.charset.StandardCharsets
import java.nio.file.{AccessDeniedException, Files, Paths}
import java.time.format.DateTimeFormatter
import scala.collection.mutable
import scala.util.control.Breaks.{break, breakable}


object calculateKPIofOpendata {


  def main(args: Array[String]): Unit = {


    var temporary_path:String=""
    var work_path:String=""
    var business_Path:String=""

   // Data directory management

    if(args.length<1){
      temporary_path="data/raw/"
      work_path="data/work/"
      business_Path="data/business/"
    }
    else if (args.length==1){
      temporary_path=args(0)+"/raw/"
      work_path=args(0)+"/work/"
      business_Path=args(0)+"/business/"
    }
    else{
      println("Too much parameter Used for that Application:\n -You have to use that application:\n either with one parameter representing the Path where you want to store the Data\n Or without any parameter")
      break()
    }

    println("temporary_path:"+temporary_path)
    println("Business_path:"+business_Path)
    println("Work_path:"+work_path)
// Data retrieving from opendata

    val current_date = DateTimeFormatter.ofPattern("ddMMYYYY").format(java.time.LocalDate.now)
    var dataframe:DataFrame=null
    GetDataFromApi(temporary_path+ current_date + "/", "bor_erp.csv", url = "https://opendata.bordeaux-metropole.fr/api/v2/catalog/datasets/bor_erp/exports/csv?limit=-1&offset=0&timezone=UTC") match{
        case Some(dataframe)=>println("No issue")

          // Data processing from raw:
          val newDataframe = dataframe.withColumn("type", when(col("type").isNull, lit("NOTR")).otherwise(col("type")))

          // Save that dataframe as parquet in directory work (Question 1)
          newDataframe.write.partitionBy("type").mode(SaveMode.Overwrite)
            .parquet(work_path + current_date )

          // Calculate Aggregation: the maximum capacity of visitors per street, with or without accommodation (Question 2)
          val kpiDataframe = clean_data(newDataframe)
            .groupBy(col("adresse_complete") , col("avec_hebergement"))agg(functions.sum(col("nb_visiteurs_max")))
            .as("nombre_visiteurs_par_rue")

        // ________________________________________________________________________________________________
        // Store the dataset in file csv (Question 3)
        kpiDataframe.repartition(1)
          .write
          .format("com.databricks.spark.csv")
          .option("delimiter", ";")
          .option("header", "true")
          .mode(SaveMode.Overwrite)
          .csv(business_Path +current_date )
        //____________________________________________________________________________________________________

        case None=>println("Permission Denied")
          break()
      }


    //) Data processing from raw:

    //val newDataframe = dataframe.withColumn("type", when(col("type").isNull, lit("NOTR")).otherwise(col("type")))

    // Save that dataframe as parquet in directory work (Question 1)
   //newDataframe.write.partitionBy("type").mode(SaveMode.Overwrite)
    //.parquet(work_path + current_date )


    //_________________________________________________________________________________________________

    // Calculate Aggregation: the maximum capacity of visitors per street, with or without accommodation (Question 2)

   /* val kpiDataframe = clean_data(newDataframe)
      .groupBy(col("adresse_complete") , col("avec_hebergement"))agg(functions.sum(col("nb_visiteurs_max")))
      .as("nombre_visiteurs_par_rue")*/


    // ________________________________________________________________________________________________
    // Store the dataset in file csv (Question 3)
    /*kpiDataframe.repartition(1)
      .write
      .format("com.databricks.spark.csv")
      .option("delimiter", ";")
      .option("header", "true")
      .mode(SaveMode.Overwrite)
      .csv(business_Path +current_date )*/
   //____________________________________________________________________________________________________

  }

  private val wordChange = mutable.HashMap("ALLEE"->"ALLEES","AV"->"AVENUE","BD"->"BOULEVARD", "RUES"->"RUE")
  private var listOfBasicStreetName=Array("BOULEVARD", "AVENUE", "ALLEE", "ALLEES", "PLACE", "QUAI", "COURS", "RUE","PARVIS", "TOUR","BD", "AV", "ROUTE")

  /**
   *This function aims is to standardize the street with common names
   * @param street, the street Name to standardize
   * @return the corresponding StreetName
   */
  private def standardizeStreetName(street:String): String={

    var rightName:String=""
    var rightNameMap=wordChange
    if(rightNameMap.contains(street)){
      rightNameMap.get(street) match{
        case Some(rightName)=>println(rightName)
        case None=>println("No value")
      }
    }
    rightName
  }

  /**
   * This function aims is to retrieve the Street's Name based on a given address
   *
   * @param address: the address from which we extract the StreetName
   * @return the StreetName extracted
   */
  def findStreetName(address : String): String = {
    var streetWords = address.split("\\s")
    val nb_words=streetWords.length
    var result:String=""
    breakable {
      for(i<-0 to streetWords.length-1){
        if(listOfBasicStreetName.contains(streetWords(i).toUpperCase)){

          if( standardizeStreetName(streetWords(i)) isEmpty){
            streetWords=streetWords.drop(i)
          }
          else {
            streetWords(i)=standardizeStreetName(streetWords(i))
            streetWords=streetWords.drop(i)
          }
          break()
        }
      }
    }
    if(nb_words==streetWords.length){
      result="RUE NON RECONNUE"
    }
    else{
      result=streetWords.mkString(" ")
    }
    result
  }

  val findStreetNameUDF : UserDefinedFunction = udf {(col1: String)
  =>findStreetName(col1)}         // transform  findStreetName to UDF function


  var ss: SparkSession = null

  /**
   * This function initialize and creates a spark session
   *
   * @param env      This parameter indicates the deployment environment our application
   *                 env = true : the deployment environment is local
   *                 env = false : the deployment environment is cluster
   * @return ss
   */

  def spark_session(env: Boolean = true): SparkSession = {

    if (env ) {
      ss = SparkSession.builder()
        .master("local[*]")
        .config("spark.sql.crossJoin.enabled", "true")
        //.enableHiveSupport()
        .getOrCreate()

    }
    else {
      ss = SparkSession.builder()
        .appName("application")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.crossJoin.enabled", "true")
        .enableHiveSupport()
        .getOrCreate()
    }
    ss
  }

  /**
   * This function allows to get the dataset from opendata platform and creates a dataframe
   * @param tmppath:  the parameter is the dataset directory
   * @param filename: name of dataset
   * @param url : url generate by API Opendata
   * @return dataframe
   */
  private def GetDataFromApi(tmppath: String, filename: String, url: String): Option[DataFrame] = {
    try {
      // Get data from API to stock
      val result = scala.io.Source.fromURL(url).mkString //  dataset recovery
      Some(Files.createDirectories(Paths.get(tmppath)))// create directory if it not exist
      Files.write(Paths.get(tmppath + filename), result.getBytes(StandardCharsets.UTF_8)) //write dataset with name in directory

      // load csv file to dataframe with spark
      val df = spark_session(true).read
        .format("com.databricks.spark.csv")
        .option("delimiter", ";")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(tmppath)
      Some(df)
    }
    catch{
      case e:AccessDeniedException =>None
    }
  }

  /**
   *This function aims is to clean the dataset
   * @param df The dataframe to cleaning
   * @return dataframe
   */
  private def clean_data(df: DataFrame): DataFrame = {
    // Exclude where type is null and adresse_1 is not null

    val df_cleaned = df.filter( "adresse_1 is not null ")  // extract addresses are not null
      .withColumn("avec_hebergement", expr("" +
      "case when avec_hebergement = '1' or  avec_hebergement =  'O' then 'avec_hebergement' " +
      "else 'sans_hebergement' end"))                                  // Clean hebergement with this rule
      .filter(col("code_postal").rlike("^3[0-9]{3,4}"))
      .withColumn("adresse_1", regexp_replace(col("adresse_1"),"/,-;.", " ")) // removing special characters
      .withColumn("adresse_1", expr("trim(lower(adresse_1))"))
      .withColumn("commune", expr("trim(lower(commune))"))
      .withColumn("adresse_1"  , findStreetNameUDF(col("adresse_1"))) // identification of streets
      .withColumn("adresse_complete", concat_ws(" ",col("adresse_1"),col("code_postal"),col("commune")))
    // Clean adress, code_postal and commune and create one adresss
    df_cleaned
  }

}
