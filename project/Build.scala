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
    anorm
  )


  val main = play.Project(appName, appVersion, appDependencies).settings(
    // Add your own project settings here      
  )

}
