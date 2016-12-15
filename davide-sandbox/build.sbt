import sbtassembly.MergeStrategy

name := "income"

organization := "org.uw"

version := "1.0"

// Scala
scalaVersion := "2.10.4"

scalacOptions += "-target:jvm-1.7"

// Java
javacOptions in (compile) ++= Seq("-source", "1.7", "-target", "1.7")

// Merge strategy: discard duplicates.
assemblyMergeStrategy in assembly := {
    case x if x.startsWith("META-INF") => MergeStrategy.discard
    case _ => MergeStrategy.first
}

// Logging
libraryDependencies +=  "log4j" % "log4j" % "1.2.17" % "provided"

// Spark/hadoop
libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.1" % "provided"

libraryDependencies += "org.apache.spark" %% "spark-streaming" % "1.6.1" % "provided"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.6.1" % "provided"

libraryDependencies += "com.databricks" % "spark-csv_2.10" % "1.4.0"

// Testing

parallelExecution in Test := false

libraryDependencies += "org.scalatest" %% "scalatest" % "2.0" % "test"

libraryDependencies += "com.novocode" % "junit-interface" % "0.11" % "test->default"