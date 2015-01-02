package biz.infoshore.spark.Trends

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.twitter.TwitterUtils
import java.util.Date

/**
 * @author Pankaj Narang infoshore
 */
object CollectTwitterData {
  //this case class holds the data which will be fetched from twitter tweets
  case class tweetInfo(tweetText:String, hashtags:Seq[String], Url:Seq[String], userName:String,
                       usersMentioned:Seq[String],retweets:Long, postedDate:Long)
  /* case class tweetInfo(tweetText:String, hashtags:String, Url:String, userName:String,
                       usersMentioned:String,retweets:Long, postedDate:Long)
  */
  def main(args: Array[String]): Unit = {
      if(args.length !=1){
        println("File For Trends Keywords Not Found")
        System.exit(1)
      }
      //optional so we can see our println statement
      StreamingLogger.setStreamingLogLevels()
      val conf = new SparkConf().setMaster("local[4]").setAppName("CSVFile")
      val ssc = new StreamingContext(conf, Seconds(300))
      var tweetStream  = TwitterUtils.createStream(ssc, None, getKeywords(args(0).trim()))
      var tweets = tweetStream.map(tweet => {
        var urls = tweet.getURLEntities.map (url=> url.getExpandedURL)
        var hashTagsKeywords = tweet.getText.split(" ").filter(_.startsWith("#"))
        var usersMentioned= tweet.getUserMentionEntities.map (x => x.getName)
        tweetInfo(tweet.getText.trim,hashTagsKeywords,urls,tweet.getUser.getName, usersMentioned,
                  tweet.getRetweetCount,tweet.getCreatedAt.getTime)
      } )
      
       //now we need to store these tweets  in files so
      //later on analysis can be done on basis of day week month years for the collected data from tweets

      val sqlContext = new org.apache.spark.sql.SQLContext(ssc.sparkContext)
      sqlContext.setConf("spark.sql.parquet.binaryAsString", "true")

    // createSchemaRDD is used to implicitly convert an RDD to a SchemaRDD.
    import sqlContext.createSchemaRDD
   tweets.foreachRDD(tweetsRDD => {tweetsRDD.distinct()
                                   val count = tweetsRDD.count
                                   println("*****" +"%s tweets found on this RDD".format(count))
      // only makes sense to create a parquet file if there is data on the RDD
                                  if (count > 0)
                                    tweetsRDD.saveAsParquetFile("urls/"+ System.currentTimeMillis) + ".parquet" })
    // Checkpoint directory to recover from failures
   println("tweets for the last stream are saved which can be processed later")
   val checkpointDir = "./checkpoint/"
    ssc.checkpoint(checkpointDir)
    ssc.start()
    ssc.awaitTermination()


  }

  def getKeywords(fileName: String) = {
    scala.io.Source.fromFile(fileName).getLines.toList
  }


}
