package biz.infoshore.spark.Trends

/**
 * @author infoshore
 */
import org.apache.spark.Logging

import org.apache.log4j.{Level, Logger}


object StreamingLogger extends Logging {

  def setStreamingLogLevels() {
    val log4jInitialized = Logger.getRootLogger.getAllAppenders.hasMoreElements
    if (!log4jInitialized) {
      
      logInfo("Setting log level to [WARN] so we can see logs easily." +
        " To override add a custom log4j.properties to the classpath.")
      Logger.getRootLogger.setLevel(Level.WARN)
    }
  }
}
