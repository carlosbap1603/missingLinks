name := "missingLinks"

version := "0.1"

scalaVersion := "2.11.12"

val sparkVersion = "2.4.5"
val breezeVersion = "1.0"

resolvers += "Spark Packages Repo" at "https://mvnrepository.com/artifact/"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-graphx" % sparkVersion,
    "org.apache.spark" %% "spark-avro" % sparkVersion,
  "org.scalanlp" %% "breeze" % breezeVersion,
  "org.scalanlp" %% "breeze-natives" % breezeVersion,
  "org.rogach" %% "scallop" % "3.3.1"
)