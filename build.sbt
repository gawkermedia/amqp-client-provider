import scalariform.formatter.preferences._
import com.typesafe.sbt.SbtScalariform.ScalariformKeys
import org.scalastyle.sbt.ScalastylePlugin

name := "amqp-client-provider"

version := "3.1.1" + (if (RELEASE_BUILD) "" else "-SNAPSHOT")

organization := "com.kinja"

crossScalaVersions := Seq("2.12.6", "2.11.8")

scalaVersion := crossScalaVersions.value.head

scalacOptions ++= Seq(
    "-unchecked",            // Show details of unchecked warnings.
    "-deprecation",          // Show details of deprecation warnings.
    "-feature",              // Show details of feature warnings.
    "-Xfatal-warnings",      // All warnings should result in a compiliation failure.
    "-Ywarn-dead-code",      // Fail when dead code is present. Prevents accidentally unreachable code.
    "-encoding", "UTF-8",    // Set correct encoding for Scaladoc.
    "-Xfuture",              // Disables view bounds, adapted args, and unsound pattern matching in 2.11.
    "-Yno-adapted-args",     // Prevent implicit tupling of arguments.
    "-Ywarn-value-discard",  // Prevent accidental discarding of results in unit functions.
    "-Xmax-classfile-name", "140"
)

javacOptions ++= Seq(
    "-Xlint:deprecation"
)

incOptions := incOptions.value.withNameHashing(true)

updateOptions := updateOptions.value.withCachedResolution(true)

val akkaVersion = "2.5.11"

libraryDependencies ++= Seq(
    "com.kinja" %% "amqp-client" % "2.0.1",
    "com.typesafe.akka" %% "akka-actor" % akkaVersion % Provided,
    "ch.qos.logback" % "logback-classic" % "1.0.0" % Provided,
    // Test dependencies
    "org.specs2" %% "specs2-core" % "4.2.0" % Test,
    "org.specs2" %% "specs2-junit" % "4.2.0" % Test,
    "org.specs2" %% "specs2-mock" % "4.2.0" % Test,
    "org.specs2" %% "specs2-scalacheck" % "4.2.0" % Test,
    "com.h2database" % "h2" % "1.4.187" % Test
)

// code formatting
SbtScalariform.scalariformSettings ++ Seq(
    ScalariformKeys.preferences := ScalariformKeys.preferences.value
        .setPreference(IndentWithTabs, true)
        .setPreference(DanglingCloseParenthesis, Preserve)
        .setPreference(DoubleIndentClassDeclaration, false)
)

// Scala linting to help preventing bugs
wartremoverErrors ++= Warts.allBut(
    Wart.Equals,
    Wart.Overloading,
    Wart.DefaultArguments,
    Wart.Throw,
    Wart.Any,
    Wart.ImplicitParameter)
