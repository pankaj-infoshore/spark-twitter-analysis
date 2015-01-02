import AssemblyKeys._ // put this at the top of the file

seq(assemblySettings: _*)

name := "Trends"

version := "1.0"

scalaVersion := "2.11.4"

libraryDependencies+="org.apache.spark" %% "spark-core" % "1.2.0"

libraryDependencies += "org.apache.spark" % "spark-streaming_2.10" % "1.2.0"

libraryDependencies += "org.apache.spark" % "spark-streaming-twitter_2.10" % "1.2.0"

libraryDependencies += "org.apache.spark" % "spark-sql_2.10" % "1.2.0"

assemblySettings

mergeStrategy in assembly := {
  case m if m.toLowerCase.endsWith("manifest.mf")          => MergeStrategy.discard
  case m if m.toLowerCase.matches("meta-inf.*\\.sf$")      => MergeStrategy.discard
  case "log4j.properties"                                  => MergeStrategy.discard
  case m if m.toLowerCase.startsWith("meta-inf/services/") => MergeStrategy.filterDistinctLines
  case "reference.conf"                                    => MergeStrategy.concat
  case _                                                   => MergeStrategy.first
}
