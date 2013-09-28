import sbt._
import Keys._
import play.Project._

object ApplicationBuild extends Build {

  val appName         = "exsim"
  val appVersion      = "1.0-SNAPSHOT"

  val appDependencies = Seq(
    "org.fusesource.scalate" %% "scalate-core" % "1.6.1",
    "com.typesafe.akka" %% "akka-actor" % "2.2.0",
    jdbc,
    anorm,
    "quickfixj" % "quickfixj-core" % "1.5.3",
    "quickfixj" % "quickfixj-msg-fix42" % "1.5.3",
    "org.apache.mina" % "mina-core" % "1.1.0"
  )


  val main = play.Project(appName, appVersion, appDependencies).settings(
      resolvers += "quickfix" at "http://repo.marketcetera.org/maven"
    // Add your own project settings here      
  )

}
