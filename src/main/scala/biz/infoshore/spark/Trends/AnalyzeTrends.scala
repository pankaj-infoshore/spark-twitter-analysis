package biz.infoshore.spark.Trends

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SchemaRDD
import scala.collection.mutable.Buffer
import org.apache.spark.SparkContext._


object AnalyzeTrends {
  
  def  main(args: Array[String]) = {
  
     val conf = new SparkConf().setMaster("local[2]").setAppName("Trends")
     val sc:SparkContext = new SparkContext(conf)
     val sqlContext = new SQLContext(sc)
     var tweets = fetchTweetRDD("/home/infoshore/java/Trends/urls",sqlContext)
  
  }

  
 def fetchTweetRDD(path: String, sqlContext: SQLContext): Unit = {
     var path ="/home/infoshore/java/Trends/urls" 
     var files =new java.io.File(path).listFiles() 
      //fetch all the parquet files created
     var parquetFiles =  files.filter (file => file.isDirectory).map(file=>file.getName)
     var tweetsRDD= parquetFiles.map(pfile=>  sqlContext.parquetFile(path+"/"+pfile))
     var allTweets =tweetsRDD.reduce((s1,s2)=>s1.unionAll(s2))
     allTweets.registerAsTable("tweets")
     sqlContext.cacheTable("tweets")
     import sqlContext._
     val popularHashTags = sqlContext.sql("SELECT hashtags,usersMentioned,Url FROM tweets")
     fetchPopularHashTags(popularHashTags);
  } 

  def fetchPopularHashTags(popularHashTags: SchemaRDD) = {
   var hashTagsList =  popularHashTags.flatMap ( x => x.getAs[Seq[String]](0)) 
   var tags =  hashTagsList.filter(tag => ! (tag.isEmpty)).map(tag=>(tag,1)).reduceByKey((a,b) => a+ b)
   var tagsSortedByCount =tags.map({case(tag,count)=>(count,tag)}).sortByKey(ascending =false)
   
   
   var userList =  popularHashTags.flatMap ( x => x.getAs[Seq[String]](1)) 
   var users =  userList.filter(user => ! (user.isEmpty)).map(user=>(user,1)).reduceByKey((a,b) => a+ b)
   var usersSortedByCount =users.map({case(user,count)=>(count,user)}).sortByKey(ascending =false)
  
    var urlList =  popularHashTags.flatMap ( x => x.getAs[Seq[String]](2)) 
   var urls =  urlList.filter(url => ! (url.isEmpty)).map(url=>(url,1)).reduceByKey((a,b) => a+ b)
   var urlCount =urls.map({case(url,count)=>(count,url)}).sortByKey(ascending =false)
   
   
     tagsSortedByCount.take(10)
     usersSortedByCount.take(10)
     urlCount.take(10)
  }

  
}