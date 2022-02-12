import javafx.beans.binding.Bindings.{concat, notEqual, select}
import org.apache.spark.sql.catalyst.ScalaReflection.Schema
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.expressions.codegen.FalseLiteral
import org.apache.spark.sql.functions.{array_distinct, col, concat_ws, current_date, exp, expr, lit, not, regexp_replace, udf, when}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.time.format.DateTimeFormatter


object devcalculate {


  def main(args: Array[String]): Unit = {


    val current_date = DateTimeFormatter.ofPattern("ddMMYYYY").format(java.time.LocalDate.now)
    GetDataFromApi("data/raw/" + current_date + "/", "bor_erp.csv", url = "https://opendata.bordeaux-metropole.fr/api/v2/catalog/datasets/bor_erp/exports/csv?limit=-1&offset=0&timezone=UTC")

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
    val dataframe = spark_session(true).read
      .format("com.databricks.spark.csv")
      .option("delimiter", ";")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("data/raw/12022022/bor_erp.csv")

    //) Data processing from raw:

    val newDataframe = dataframe.withColumn("type", when(col("type").isNull, lit("NOTR")).otherwise(col("type")))

    // Save tha dataframe as parquet in directory work
    newDataframe.write.partitionBy("type").mode(SaveMode.Overwrite)
     .parquet("data/work/" + current_date )

    //val df = newDataframe.where(col("code_postal").rlike("[3]{2}[0-9]{3}")).select(col("code_postal")).distinct()
    // ________________________________________________________________________________________________
    val df = clean_data(dataframe)
/**
    val adresseList = df.select(col("adresse_1"))
      .collect()
      .toList
      .map(e=>e)
 //val dre : List[String] = adresseList.map(e => List(e))

 println(adresseList)
**/
  /**Ferdinand*/
    val liste=Array("BOULEVARD", "AVENUE", "ALLEE", "ALLEES", "PLACE", "QUAI", "COURS", "RUE","PARVIS", "TOUR","BD", "AV")
    val adresseList=df("adresse_1")

    /**Ferdinand*/
   df.show()

  }


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
    return ss
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
   *
   * @param df
   * @return
   */
  private def clean_data(df: DataFrame): DataFrame = {
    // Exclude where type is null and adresse_1 is not null
    val df_filtered = df.filter("adresse_1 is not null ")
    // Clean hebergement with this rule
    val df_cleaned = df_filtered.withColumn("avec_hebergement", expr("" +
      "case when avec_hebergement = '1' or  avec_hebergement =  'O' then 'avec_hebergement' " +
      "else 'sans_hebergement' end"))
      //.withColumn("code_postal", when(col("code_postal")
       // .rlike("[3][0][0-9]{2}"), lit((concat("3", col("code_postal").toString() )) )).otherwise(col("code_postal")))
      .withColumn("commune", when(col("commune").isNull, lit("BORDEAUX")).otherwise(col("commune")))
      .withColumn("adresse_1", expr("trim(lower(adresse_1))"))
      .withColumn("commune", expr("trim(lower(commune))"))
      .filter(col("adresse_1").isNotNull)
      //.withColumn("adresse_complete", concat_ws(" ",col("adresse_1"),col("code_postal"),col("commune")))
    // Clean adress, code_postal and commune and create one adresss
    // add other cleansing to improve data quality
    // add other cleansing to improve data quality
    return df_cleaned
  }
  /**
   def dataCleanedOfAdress( dataFrame: DataFrame) : DataFrame= {


   }
   **/

}
