logLevel := Level.Warn

resolvers += "Bintray sbt plugin releases" at "http://dl.bintray.com/sbt/sbt-plugin-releases/"

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.1")

addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.3.5")

addSbtPlugin("org.scalastyle" %% "scalastyle-sbt-plugin" % "0.8.0")