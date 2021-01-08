name := "BoardGameAnalysis"

version := "0.1"

scalaVersion := "2.12.10"


libraryDependencies += "org.scalaj" % "scalaj-http_2.11" % "2.3.0"
libraryDependencies += "org.apache.httpcomponents" % "httpclient" % "4.5.12"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.0.1"
libraryDependencies +=  "com.amazonaws" % "aws-java-sdk" % "1.11.46"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}