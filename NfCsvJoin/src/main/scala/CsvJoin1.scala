import org.apache.spark.sql.types.{StructType, _}
import org.apache.spark.sql.{SQLContext, _}
import org.apache.spark.sql.SQLContext._
import org.apache.spark.{SparkConf, SparkContext, _}
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.rdd._

object SparkCsvJoin1 {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("sparkCsvJoin").setMaster("local[2]")
    val sparkContext = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sparkContext)
    import sqlContext.implicits._
    
    // This sample assumes that the data files needed are available at the relative path
    val f1data = sparkContext.textFile("/home/afcamar/gitlab/datamonk/spark/scalaCsvJoin/data/nfsample.csv")
    val f2data = sparkContext.textFile("/home/afcamar/gitlab/datamonk/spark/scalaCsvJoin/data/sccmsample.csv")
    
    def f1dataHeaderFormat(line: String) :Boolean = {
      line.contains("sa,da,subnet,")
    }
    def f2dataHeaderFormat(line: String) :Boolean = {
      line.contains("ip_addr,os_type,os_version,")
    }
                              
    // You need to get the headers and use them for the schema
    // NOTE: you need to see how to deal with names that have commas.
    val f1dataHeader = f1data.filter(f1dataHeaderFormat(_)).collect()(0).split(",")
    val f2dataHeader = f2data.filter(f2dataHeaderFormat(_)).collect()(0).split(",")
    
    // Simplistic Schema creation - we assume they are all nullable Strings and you notice here
    // that we don't have the nice type inferencing for free like with JSON.
    val f1schema = StructType(f1dataHeader.map(fieldName => StructField(fieldName, StringType, true)))
    val f2schema = StructType(f2dataHeader.map(fieldName => StructField(fieldName, StringType, true)))
    
    val joinedSchema = StructType(Array(
      StructField("sa", StringType, true),
      StructField("da", StringType, true),
      StructField("subnet", StringType, true),
      StructField("mac1", StringType, true),
      StructField("ip_addr", StringType, true),
      StructField("os_type", StringType, true),
      StructField("os_version", StringType, true),
      StructField("mac_addr", StringType, true)))
    
    // Get just the data
    val f1dataLines = f1data.filter(!f1dataHeaderFormat(_))
    val f2dataLines = f2data.filter(!f2dataHeaderFormat(_))

    // Make Rows and don't break quoted string values.
    val f1rowRDD = f1dataLines.map(p=>{Row.fromSeq(p.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)"))})
    val f2rowRDD = f2dataLines.map(p=>{Row.fromSeq(p.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)"))})
    
    // Now we can make a DataFrame and get back to SQL processing in Spark
    val f1dataDF = sqlContext.createDataFrame(f1rowRDD, f1schema)
    val f2dataDF = sqlContext.createDataFrame(f2rowRDD, f2schema)
    
    //sp500financialsDF.registerTempTable("sp500fin")
    f1dataDF.registerTempTable("f1dataIn_c")
    f2dataDF.registerTempTable("f2dataIn_c")
    
    //val all =  sqlContext.sql("SELECT * FROM sp500fin")
    //all.show
    //val f1dataAll =  sqlContext.sql("SELECT * FROM f1dataIn_c")
    sqlContext.sql("CACHE TABLE f1dataIn_c").collect()
    val f1dataAll =  sqlContext.sql("SELECT * FROM f1dataIn_c")
    
    sqlContext.sql("CACHE TABLE f2dataIn_c").collect()
    val f2dataAll =  sqlContext.sql("SELECT * FROM f2dataIn_c")
    //f1dataAll.show
    //val f2dataAll =  sqlContext.sql("SELECT * FROM f2dataIn")
    //f2dataAll.show
    
    // test join of both dataframes
    //f1dataDF.join(f2dataDF, f1dataDF("number") <=> f2dataDF("number") && f1dataDF("type") <=> f2dataDF("type"), "outer").show()
    val joinedData = f1dataAll.join(f2dataAll, f1dataAll("sa") <=> f2dataAll("ip_addr"), "outer")
    
    //val joinedDataDF = sqlContext.createDataFrame(joinedData, joinedSchema)
    //val joinedColumnSelect = joinedData.select("sa", "subnet", "os_type", "os_version").show()
    
    val joinedColumnWriter = joinedData.select("sa", "subnet", "os_type", "os_version").write
    .format("com.databricks.spark.csv")
    .option("header", "false")
    .save("data/out")
    
    //joinedData.filter(joinedData("sa").isNotNull()).show()
    
    //  df.filter( df("columnname").isNotNull() )
    
    //f1dataDF.join(f2dataDF, f1dataDF("sa") <=> f2dataDF("ip_addr"), "outer").show()
    
    //val avgEPSAnon = sqlContext.sql("SELECT AVG(`Earnings/Share`) FROM sp500fin")
    //val avgEPSNamed = sqlContext.sql("SELECT AVG(`Earnings/Share`) as AvgEPS FROM sp500fin")
    //val avgEPSProg = sp500financialsDF.agg(avg(sp500financialsDF.col("Earnings/Share")))
  }
}
