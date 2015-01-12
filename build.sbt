lazy val root = (project in file(".")).
  settings(
    name := "TwitterServer",
    version := "1.0",
    scalaVersion := "2.11.4",
    unmanagedBase := baseDirectory.value/"lib",
    resolvers += "spray repo" at "http://repo.spray.io",
    libraryDependencies ++= Seq(
  	  "io.spray" %% "spray-routing" % "1.3.1",
  	  "io.spray" %% "spray-can" % "1.3.1",
	  "org.scala-lang.modules" %% "scala-parser-combinators" % "1.0.2",
	  "org.json4s"    %% "json4s-native"   % "3.2.11"))

