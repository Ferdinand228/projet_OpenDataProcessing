import javafx.beans.binding.Bindings.{concat, notEqual, select}
import org.apache.spark.sql.catalyst.ScalaReflection.Schema
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession, functions}
import org.apache.spark.sql.catalyst.expressions.codegen.FalseLiteral
import org.apache.spark.sql.functions.{array_distinct, col, concat_ws, current_date, exp, expr, lit, not, regexp_replace, udf, when}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.time.format.DateTimeFormatter
import scala.collection.mutable
import scala.util.control.Breaks.{break, breakable}


object devcalculate {


  def main(args: Array[String]): Unit = {


    val current_date = DateTimeFormatter.ofPattern("ddMMYYYY").format(java.time.LocalDate.now)
    //GetDataFromApi("data/raw/" + current_date + "/", "bor_erp.csv", url = "https://opendata.bordeaux-metropole.fr/api/v2/catalog/datasets/bor_erp/exports/csv?limit=-1&offset=0&timezone=UTC")

    // load csv file to dataframe with spark

    // The structure schema of the bor_erp.csv
    /**
    val schemaData = StructType(Array(
      StructField("nom_etablissement", StringType, true),
      StructField("adresse_1", StringType, true),
      StructField("adresse_2", StringType, true),
      StructField("commune", StringType, true),
      StructField("canton", StringType, true),
      StructField("code_postal", IntegerType, true),
      StructField("categorie", IntegerType, true),
      StructField("avec_hebergement", StringType, true),
      StructField("effectif_personnel", IntegerType, false),
      StructField("propriete_ville", StringType, false),
      StructField("nb_visiteurs_max", IntegerType, false),
      StructField("type", StringType, false),
      StructField("geometrie", StringType, false)))
     **/
    //create a dataframe
    val dataframe = spark_session().read
      .format("com.databricks.spark.csv")
      .option("delimiter", ";")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("data/raw/12022022/bor_erp.csv")

    //) Data processing from raw:

    val newDataframe = dataframe.withColumn("type", when(col("type").isNull, lit("NOTR")).otherwise(col("type")))

    // Save tha dataframe as parquet in directory work
   // newDataframe.write.partitionBy("type").mode(SaveMode.Overwrite)
   //  .parquet("data/work/" + current_date )

    //val df = newDataframe.where(col("code_postal").rlike("[3]{2}[0-9]{3}")).select(col("code_postal")).distinct()

    //_________________________________________________________________________________________________
    val df = clean_data(dataframe)
    val dfj = df.withColumn("adresse_1"  , findStreetNameUDF(col("adresse_1")))
    val dfs = dfj.groupBy(col("adresse_1") , col("avec_hebergement")).agg(functions.sum(col("nb_visiteurs_max")))

    // ________________________________________________________________________________________________
    dfs.repartition(1)
      .write
      .format("com.databricks.spark.csv")
      .option("delimiter", ";")
      .option("header", "true")
      .mode(SaveMode.Overwrite)
      .csv("data/business/" +current_date )
   //____________________________________________________________________________________________________
    //val dfr =dfj.filter(col("code_postal")==="3code_postal")
    //dfj.select(col("code_postal")).distinct().show(40)
  }

  private val wordChange = mutable.HashMap("ALLEE"->"ALLEES","AV"->"AVENUE","BD"->"BOULEVARD", "RUES"->"RUE")
  private var listOfBasicStreetName=Array("BOULEVARD", "AVENUE", "ALLEE", "ALLEES", "PLACE", "QUAI", "COURS", "RUE","PARVIS", "TOUR","BD", "AV")

  /**
   *This function aims is to standardize the street with common names
   * @param street, the street Name to standardize
   * @return the corresponding StreetName
   */
  def standardizeStreetName(street:String): String={

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
   * @param app_name This parameter is the name of the application
   * @return ss
   */

  def spark_session(env: Boolean = true, app_name: String = " "): SparkSession = {

    if (env == true && app_name== " ") {
      ss = SparkSession.builder()
        .master("local[*]")
        .config("spark.sql.crossJoin.enabled", "true")
        //.enableHiveSupport()
        .getOrCreate()

    }
    else {
      ss = SparkSession.builder()
        .appName("app_name")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.crossJoin.enabled", "true")
        .enableHiveSupport()
        .getOrCreate()
    }
    ss
  }

  /**
   * This function allows to get the dataset from opendata platform
   * @param tmppath:  the parameter is the dataset directory
   * @param filename: name of dataset
   * @param url : url generate by API Opendata
   */
  private def GetDataFromApi(tmppath: String, filename: String, url: String): Unit = {
    // Get data from API to stock
    val result = scala.io.Source.fromURL(url).mkString //  dataset recovery
    Files.createDirectories(Paths.get(tmppath)) // create directory if it not exist
    Files.write(Paths.get(tmppath + filename), result.getBytes(StandardCharsets.UTF_8)) //write dataset with name in directory
  }

  /**
   *This function aims is to clean the dataset
   * @param df
   * @return
   */
  private def clean_data(df: DataFrame): DataFrame = {
    // Exclude where type is null and adresse_1 is not null

    val df_cleaned = df.filter( "adresse_1 is not null ")  // extract addresses are not null
      .withColumn("avec_hebergement", expr("" +
      "case when avec_hebergement = '1' or  avec_hebergement =  'O' then 'avec_hebergement' " +
      "else 'sans_hebergement' end"))                                  // Clean hebergement with this rule
      .filter(col("code_postal").rlike("^3[0-9]{3,4}"))
      .withColumn("adresse_1", regexp_replace(col("adresse_1"),"/,-;.", " ")) // removing special characters
      //.withColumn("commune", when(col("commune").isNull, lit("BORDEAUX")).otherwise(col("commune")))
      .withColumn("adresse_1", expr("trim(lower(adresse_1))"))
      .withColumn("commune", expr("trim(lower(commune))"))

      //.withColumn("adresse_complete", concat_ws(" ",col("adresse_1"),col("code_postal"),col("commune")))
    // Clean adress, code_postal and commune and create one adresss
    df_cleaned
  }

}
