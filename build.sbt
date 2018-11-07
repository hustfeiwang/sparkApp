name := "SparkApplication"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.1.2"

libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.1.2"

libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.1.2" % "provided"

resolvers ++= Seq(
  "ali" at "http://maven.aliyun.com/nexus/content/groups/public/",
  "Sonatype Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/",
  "Sonatype Release" at "https://oss.sonatype.org/content/repositories/releases/"
)
