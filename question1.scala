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

//Dataframe of 9gag data
val gagRdd1 = cleanRdd1.map( x => gag(x._1.substring(0, 25), x._2.substring(1), x._3.substring(1), x._4.substring(1))).toDF
val gagRdd2 = cleanRdd2.map( x => gag(x._1.substring(0, 25), x._2.substring(1), x._3.substring(1), x._4.substring(1))).toDF
val gagRddTV1 = cleanRddTV1.map( x => gag(x._1.substring(0, 25), x._2.substring(1), x._3.substring(1), x._4.substring(1))).toDF
val gagRddTV2 = cleanRddTV2.map( x => gag(x._1.substring(0, 25), x._2.substring(1), x._3.substring(1), x._4.substring(1))).toDF

val pairRdd1 = gagRdd1.select($"timeStamp", $"userID")
val pairRdd2 = gagRdd2.select($"timeStamp", $"userID")
val pairRddTV1 = gagRddTV1.select($"timeStamp", $"userID")
val pairRddTV2 = gagRddTV2.select($"timeStamp", $"userID")

val fullRdd = pairRdd1.union(pairRdd2)
val totalCount = fullRdd.groupBy("userID").count()
totalCount.show()
println("====The number of distinct number of users who visited both 9GAG.com====")
totalCount.count()

val fullRddTV = pairRddTV1.union(pairRddTV2)
val totalCountTV = fullRddTV.groupBy("userID").count()
totalCountTV.show()
println("====The number of distinct number of users who visited 9GAG.tv====")
totalCountTV.count()

//Join TV and com table 
val newFullRdd = fullRdd.union(fullRddTV)
val newTotalCount = newFullRdd.groupBy("userID").count()
newTotalCount.show()
println("====The number of distinct number of users who visited both 9GAG.com and 9GAG.tv====")
newTotalCount.count()
