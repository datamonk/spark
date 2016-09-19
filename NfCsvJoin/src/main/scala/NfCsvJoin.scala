/*
 * 
 * #!/bin/bash
 * PROJ_PATH="~/gitlab/datamonk/spark/NfCsvJoin"
 * TARGET_DIR="/target/scala-2.11"
 * JAR_FILE="csv-join-project_2.11-1.0.jar"
 * PKGS="com.databricks:spark-csv_2.10:1.4.0"
 * # build package w/ sbt
 * sbt package
 * sleep 5
 * 
 * # spark-submit cmd example
 * spark-submit \
 * --packages ${PKGS} \
 * --class "NfCsvJoin" \
 * --master local[2] \
 * "${PROJ_PATH}/${TARGET_DIR}/${JAR_FILE}" \
 * "${PROJ_PATH}/data/nf/<wildcard>" \
 * "${PROJ_PATH}/data/sccm/<wildcard>" \
 * "${PROJ_PATH}/data/out2"
 * 
 */
import org.apache.spark.{SparkConf, SparkContext, _}
import org.apache.spark.sql.{SQLContext, _}
import org.apache.spark.sql.SQLContext._
import org.apache.spark.sql.types.{StructType, _}
import collection.JavaConversions._

object NfCsvJoin {
  def main(args: Array[String]) {
    /* grab cmd args
     * 0 :: netflow input path (ie, /base/path/nf/)
     * 1 :: sccm input path (ie, /base/path/sccm/)
     * 2 :: output path (ie, /base/path/out/<date>/)
     */
    val nfPath = args(0)
    val sccmPath = args(1)
    val outPath = args(2)
    
    // set context for spark and sql
    val sparkConf = new SparkConf().setAppName("sparkNetflowCsvJoin").setMaster("local[2]")
    val sparkContext = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sparkContext)
    import sqlContext.implicits._
    
    // define nf schema :: (sa,da,subnet,mac1,...)
    val nfSchema = StructType(Array(
      StructField("sa", StringType, true),
      StructField("da", StringType, true),
      StructField("subnet", StringType, true),
      StructField("mac1", StringType, true)))
    
    // define sccm schema :: (ip_addr,os_type,os_version,...)
    val sccmSchema = StructType(Array(
      StructField("ip_addr", StringType, true),
      StructField("os_type", StringType, true),
      StructField("os_version", StringType, true),
      StructField("mac_addr", StringType, true)))
      
    // read in data from netflow input path, parse with databrick's spark-csv lib.  
    val nfDF = sqlContext.read
    .format("com.databricks.spark.csv")
    .option("header", "true") // Use first line of all files as header
    .schema(nfSchema)
    .load(nfPath)
    
    // read in data from sccm input path, parse with databrick's spark-csv lib.
    val sccmDF = sqlContext.read
    .format("com.databricks.spark.csv")
    .option("header", "true") // Use first line of all files as header
    .schema(sccmSchema)
    .load(sccmPath)
    
    // create a new temp table for sccm data
    sccmDF.registerTempTable("sccmDF_c")
    
    // now cache the sccm temp table for nf evaluation
    sqlContext.sql("CACHE TABLE sccmDF_c").collect()
    
    // let's select everything from the cached sccm table
    val sccmDFCached = sqlContext.sql("SELECT * FROM sccmDF_c")
    
    // perform an outer join of nf / sccm rows that match by key (sa and ip_addr).
    // once joined, only select the columns we care about and write out to filesystem (ie, outDir :: arg 3).
    val dfMerged = nfDF.join(sccmDFCached, nfDF("sa") <=> sccmDFCached("ip_addr"), "outer")
    .select("sa", "subnet", "os_type", "os_version")
    .write
    .format("com.databricks.spark.csv")
    .option("header", "false") // don't include header for output
    .save(outPath)
  }
}