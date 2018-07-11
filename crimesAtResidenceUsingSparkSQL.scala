package crimeDataAnalysis

import org.apache.spark.{SparkConf, SparkContext}

object crimesAtResidenceUsingSparkSQL {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("Get Revenue per Order")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val crimes = sc.textFile("/user/cloudera/imran/Crimes_-_2001_to_present.csv")
    val crimeDataHeader = crimes.first()
    val crimeDataWithoutHeader = crimes.filter(ele => ele != crimeDataHeader)
    val crimeDataWithoutHeaderDF = crimeDataWithoutHeader.map( rec => {
      val r = rec.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)
      (r(7), r(5))}).
      toDF("location_Description","crime_Type")
    crimeDataWithoutHeaderDF.registerTempTable("crime_data")
    sqlContext.setConf("spark.sql.shuffle.partitions", "4")
    sqlContext.sql( "select * from (" +
      "select crime_type, count(1) crime_count " +
      "from crime_data " +
      "where location_Description = 'RESIDENCE' " +
      "group by crime_type " +
      "order by crime_count desc) q " +
      "limit 3").coalesce(1).save("/user/cloudera/imran/crimeResults", "json")

  }

}
