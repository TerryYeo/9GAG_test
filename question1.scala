/** Running in Apache Zeppelin (A notebook which support Apache Spark interpreter.)
  * I have put the file into hdfs and used Apache Spark for distributed analysis
  */

val baseDir = "hdfs://c7:9000/data/terry/"
val server1 = sc.textFile(baseDir + "server1.txt")
val server2 = sc.textFile(baseDir + "server2.txt")
val serverTV1 = sc.textFile(baseDir + "server1.txt.1")
val serverTV2 = sc.textFile(baseDir + "server2.txt.1")
println("====The number of data in server1.txt from 9gag.com====")
println(server1.count)
println("====The number of data in server2.txt from 9gag.com====")
println(server2.count)
println("====The number of data in server1.txt from 9gag.tv====")
println(serverTV1.count)
println("====The number of data in server2.txt from 9gag.tv====")
println(serverTV2.count)

//Because the dataset from server1.txt and server2.txt get some wrong format data, therefore I need to filter the wrong format data by using regular expression
val regex = """^(\d{4}-\d{2}\-\d{2}T\d{2}:\d{2}\:\d{2}\+\d{2}:\d{2}\[\d{1,5}\]\[([0-9]{1,3})\.([0-9]{1,3})\.([0-9]{1,3})\.([0-9]{1,3})\])""".r
val filter1 = server1.filter(s => regex.findFirstIn(s).isDefined)
val filter2 = server2.filter(s => regex.findFirstIn(s).isDefined)
val filterTV1 = serverTV1.filter(s => regex.findFirstIn(s).isDefined)
val filterTV2 = serverTV2.filter(s => regex.findFirstIn(s).isDefined)
println("====The number of data in server1.txt from 9gag.com after filtering====")
filter1.count()
println("====The number of data in server2.txt from 9gag.com after filtering====")
filter2.count()
println("====The number of data in server1.txt from 9gag.tv after filtering====")
filterTV1.count()
println("====The number of data in server2.txt from 9gag.tv after filtering====")
filterTV2.count()

//Define a case class for 9GAG data
case class gag(timeStamp: String,ipAddress: String, userID: String, webAddress: String)

val splitRdd1 = filter1.map( x=>x.split("]"))
val cleanRdd1 = splitRdd1.map( arr => {
  val timestamp = arr(0)   
  val ipaddress = arr(1)
  val username = arr(2)
  val website = arr(3)
  (timestamp, ipaddress, username, website)
})

val splitRdd2 = filter2.map( x=>x.split("]"))
val cleanRdd2 = splitRdd2.map( arr => {
  val timestamp = arr(0)   
  val ipaddress = arr(1)
  val username = arr(2)
  val website = arr(3)
  (timestamp, ipaddress, username, website)
})

val splitRddTV1 = filterTV1.map( x=>x.split("]"))
val cleanRddTV1 = splitRddTV1.map( arr => {
  val timestamp = arr(0)   
  val ipaddress = arr(1)
  val username = arr(2)
  val website = arr(3)
  (timestamp, ipaddress, username, website)
})

val splitRddTV2 = filterTV2.map( x=>x.split("]"))
val cleanRddTV2 = splitRddTV2.map( arr => {
  val timestamp = arr(0)   
  val ipaddress = arr(1)
  val username = arr(2)
  val website = arr(3)
  (timestamp, ipaddress, username, website)
})

//Creating dataframe of 9gag data
val gagRdd1 = cleanRdd1.map( x => gag(x._1.substring(0, 25), x._2.substring(1), x._3.substring(1), x._4.substring(1))).toDF
val gagRdd2 = cleanRdd2.map( x => gag(x._1.substring(0, 25), x._2.substring(1), x._3.substring(1), x._4.substring(1))).toDF
val gagRddTV1 = cleanRddTV1.map( x => gag(x._1.substring(0, 25), x._2.substring(1), x._3.substring(1), x._4.substring(1))).toDF
val gagRddTV2 = cleanRddTV2.map( x => gag(x._1.substring(0, 25), x._2.substring(1), x._3.substring(1), x._4.substring(1))).toDF

//Just need to have timeStamp and userID to count the distinct for making sure the data won't be replaced when doing union
val pairRdd1 = gagRdd1.select($"timeStamp", $"userID")
val pairRdd2 = gagRdd2.select($"timeStamp", $"userID")
val pairRddTV1 = gagRddTV1.select($"timeStamp", $"userID")
val pairRddTV2 = gagRddTV2.select($"timeStamp", $"userID")

//Joining two 9gag.com data into one table
val fullRdd = pairRdd1.union(pairRdd2).cache()    //caching the table for further using
val totalCount = fullRdd.groupBy("userID").count()
totalCount.show()
println("====The number of distinct number of users who visited both 9GAG.com====")
totalCount.count()

//Joining two 9gag.tv data into one table
val fullRddTV = pairRddTV1.union(pairRddTV2).cache()    //caching the table for further using
val totalCountTV = fullRddTV.groupBy("userID").count()
totalCountTV.show()
println("====The number of distinct number of users who visited 9GAG.tv====")
totalCountTV.count()

//Join 9gag.tv and 9gag.com table 
val newFullRdd = fullRdd.union(fullRddTV)
val newTotalCount = newFullRdd.groupBy("userID").count()
newTotalCount.show()
println("====The number of distinct number of users who visited both 9GAG.com and 9GAG.tv====")
newTotalCount.count()





//The following code is some tips for doing streaming by using Spark Structured Streaming

/**
 * Counts words in UTF8 encoded, '\n' delimited text received from the network.
 *
 * Usage: StructuredNetworkWordCount <hostname> <port>
 * <hostname> and <port> describe the TCP server that Structured Streaming
 * would connect to receive data.
 *
 * To run this on your local machine, you need to first run a Netcat server
 *    `$ nc -lk 9999`
 * and then run the example
 *    `$ bin/run-example sql.streaming.StructuredNetworkWordCount
 *    localhost 9999`
 */
object StructuredNetworkWordCount {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: StructuredNetworkWordCount <hostname> <port>")
      System.exit(1)
    }

    val host = args(0)
    val port = args(1).toInt

    val spark = SparkSession
      .builder
      .appName("StructuredNetworkWordCount")
      .getOrCreate()

    import spark.implicits._

    // Create DataFrame representing the stream of input lines from connection to host:port
    val lines = spark.readStream
      .format("socket")
      .option("host", host)
      .option("port", port)
      .load()

    val query = newTotalCount.writeStream
      .outputMode("complete")
      .format("console")
      .start()

    query.awaitTermination()
  }
}


/**
 * These are some results from the above code:
 *
 * ====The number of data in server1.txt from 9gag.com====
 * 557119
 * ====The number of data in server2.txt from 9gag.com====
 * 542201
 * ====The number of data in server1.txt from 9gag.tv====
 * 60612
 * ====The number of data in server2.txt from 9gag.tv====
 * 60756
 * ====The number of data in server1.txt from 9gag.com after filtering====
 * res73: Long = 557092
 * ====The number of data in server2.txt from 9gag.com after filtering====
 * res75: Long = 542174
 * ====The number of data in server1.txt from 9gag.tv after filtering====
 * res77: Long = 60612
 * ====The number of data in server2.txt from 9gag.tv after filtering====
 * res79: Long = 60756
 * ====The number of distinct number of users who visited both 9GAG.com====
 * res82: Long = 41388
 * ====The number of distinct number of users who visited 9GAG.tv====
 * res85: Long = 50915
 * ====The number of distinct number of users who visited both 9GAG.com and 9GAG.tv====
 * res88: Long = 65934
 */
