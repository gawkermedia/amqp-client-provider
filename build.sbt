import scalariform.formatter.preferences._
import com.typesafe.sbt.SbtScalariform.ScalariformKeys
import org.scalastyle.sbt.ScalastylePlugin

name := "amqp-client-provider"

version := "2.3.0"

organization := "com.kinja"

scalaVersion := "2.11.8"

crossScalaVersions := Seq("2.11.8")

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

shellPrompt in ThisBuild := { state => Project.extract(state).currentRef.project + "> " }

val akkaVersion = "2.3.12"

libraryDependencies ++= Seq(
    "com.kinja" %% "amqp-client" % "1.5.1",
    "com.typesafe.akka" %% "akka-actor" % akkaVersion % Provided,
    "ch.qos.logback" % "logback-classic" % "1.0.0" % Provided,
    // Test dependencies
    "org.specs2" %% "specs2-core" % "3.7" % Test,
    "org.specs2" %% "specs2-junit" % "3.7" % Test,
    "org.specs2" %% "specs2-mock" % "3.7" % Test,
    "org.specs2" %% "specs2-scalacheck" % "3.7" % Test,
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
wartremoverErrors ++= Seq(
    Wart.IsInstanceOf,   // Prevent type-casing.
    // Wart.AsInstanceOf,   // Prevent dynamic types.
    Wart.Return,         // Prevent use of `return` keyword.
    Wart.Any2StringAdd,  // Prevent accidental stringification.
    Wart.OptionPartial,  // Option.get is unsafe.
    Wart.TryPartial,     // Try.get is unsafe.
    Wart.ListOps,        // Prevent throwing exceptions in List's functions.
    Wart.Null,           // Prevent using null.
    Wart.Product,        // Prevent incorrect generic types.
    Wart.Serializable,   // Prevent incorrect generic types.
    Wart.Var,            // Prevent using var.
    Wart.Enumeration,    // Prevent using Scala enumerations.
    Wart.ToString,       // Prevent automatic string conversion.
    Wart.FinalCaseClass, // Case classes should always be final.
    Wart.ExplicitImplicitTypes,  // Force explicit type annotations for implicits.
    Wart.EitherProjectionPartial // Prevent throwing exceptions.
)

def getEnvOrDefault(key: String, default: String): String = {
    if (System.getenv().containsKey(key)) {
        System.getenv(key)
    } else {
        default
    }
}

val CI_BUILD = System.getProperty("JENKINS_BUILD") == "true"
val artifactoryUrl = sys.env.get("KINJA_PUBLIC_REPO").getOrElse("https://kinjajfrog.jfrog.io/kinjajfrog/sbt-virtual/")

credentials += Credentials(Path.userHome / ".ivy2" / ".kinja-artifactory.credentials")

publishTo <<= (version)(version =>
    if (CI_BUILD) {
        if (version endsWith "SNAPSHOT") Some("Kinja Snapshots" at sys.env.get("KINJA_SNAPSHOTS_REPO").getOrElse("https://kinjajfrog.jfrog.io/kinjajfrog/kinja-local-snapshots/"))
        else                             Some("Kinja Releases" at sys.env.get("KINJA_RELEASES_REPO").getOrElse("https://kinjajfrog.jfrog.io/kinjajfrog/kinja-local-releases/"))
    } else {
        None
    }
)

resolvers += "Kinja Public Group" at artifactoryUrl

resolvers += "Scalaz Bintray Repo" at "http://dl.bintray.com/scalaz/releases"
