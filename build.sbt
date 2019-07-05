import scalariform.formatter.preferences._

name := "amqp-client-provider"

version := "10.0.1" + (if (RELEASE_BUILD) "" else "-SNAPSHOT")

organization := "com.kinja"

crossScalaVersions := Seq("2.12.8", "2.13.0", "2.11.12")

scalaVersion := crossScalaVersions.value.head

scalacOptions ++= Seq(
	"-unchecked",                        // Show details of unchecked warnings.
	"-deprecation",                      // Show details of deprecation warnings.
	"-encoding", "UTF-8",                // Set correct encoding for Scaladoc.
	"-feature",                          // Show details of feature warnings.
	"-explaintypes",                     // Explain type errors in more detail.
	"-Xcheckinit",                       // Wrap field accessors to throw an exception on uninitialized access.
	"-Xlint",                            // Ensure best practices are being followed.
	"-Xlint:adapted-args",               // Warn if an argument list is modified to match the receiver.
	"-Xlint:delayedinit-select",         // Selecting member of DelayedInit.
	"-Xlint:doc-detached",               // A Scaladoc comment appears to be detached from its element.
	"-Xlint:inaccessible",               // Warn about inaccessible types in method signatures.
	"-Xlint:infer-any",                  // Warn when a type argument is inferred to be `Any`.
	"-Xlint:missing-interpolator",       // A string literal appears to be missing an interpolator id.
	"-Xlint:nullary-override",           // Warn when non-nullary `def f()' overrides nullary `def f'.
	"-Xlint:nullary-unit",               // Warn when nullary methods return Unit.
	"-Xlint:option-implicit",            // Option.apply used implicit view.
	"-Xlint:package-object-classes",     // Class or object defined in package object.
	"-Xlint:poly-implicit-overload",     // Parameterized overloaded implicit methods are not visible as view bounds.
	"-Xlint:private-shadow",             // A private field (or class parameter) shadows a superclass field.
	"-Xlint:stars-align",                // Pattern sequence wildcard must align with sequence component.
	"-Xlint:type-parameter-shadow",      // A local type parameter shadows a type already in scope.
	"-Ywarn-dead-code",                  // Fail when dead code is present. Prevents accidentally unreachable code.
	"-Ywarn-dead-code",                  // Fail when dead code is present. Prevents accidentally unreachable code.
	"-Ywarn-numeric-widen",              // Warn when numerics are widened.
	"-Ywarn-value-discard"               // Prevent accidental discarding of results in unit functions.
)

// Enable compiler checks added in Scala 2.12
scalacOptions ++= (CrossVersion.partialVersion((scalaVersion in ThisProject).value) match {
	case Some((2, scalaMajor)) if scalaMajor >= 12 =>
		Seq(
			"-Xlint:constant",                   // Evaluation of a constant arithmetic expression results in an error.
			"-Ywarn-extra-implicit",             // Warn when more than one implicit parameter section is defined.
			"-Ywarn-unused:implicits",           // Warn if an implicit parameter is unused.
			"-Ywarn-unused:locals",              // Warn if a local definition is unused.
//			"-Ywarn-unused:params",              // Warn if a value parameter is unused. Disabled due to false positives.
			"-Ywarn-unused:patvars",             // Warn if a variable bound in a pattern is unused.
			"-Ywarn-unused:privates"             // Warn if a private member is unused.
		)
	case _ =>
		Seq()
})

javacOptions ++= Seq(
    "-Xlint:deprecation"
)

updateOptions := updateOptions.value.withCachedResolution(true)

val akkaVersion = "2.5.23"
val specs2Version = "4.5.1"

libraryDependencies ++= Seq(
    "com.kinja" %% "amqp-client" % "2.2.2-SNAPSHOT",
    "com.kinja" %% "warts" % "1.0.5-SNAPSHOT" % Provided,
    "com.typesafe.akka" %% "akka-actor" % akkaVersion % Provided,
    "ch.qos.logback" % "logback-classic" % "1.0.0" % Provided,
    // Test dependencies
    "org.specs2" %% "specs2-core" % specs2Version % Test,
    "org.specs2" %% "specs2-junit" % specs2Version % Test,
    "org.specs2" %% "specs2-mock" % specs2Version % Test,
    "org.specs2" %% "specs2-scalacheck" % specs2Version % Test,
    "com.h2database" % "h2" % "1.4.187" % Test
)

libraryDependencies ++= (CrossVersion.partialVersion(scalaVersion.value) match {
	case Some((2, scalaMajor)) if scalaMajor < 13 =>
	  Seq("org.scala-lang.modules" %% "scala-collection-compat" % "2.1.1")
	case _ =>
	  Seq()
})

// Code formatting
scalariformAutoformat := FORMAT_CODE
scalariformPreferences := scalariformPreferences.value
  .setPreference(IndentWithTabs, true)
  .setPreference(DanglingCloseParenthesis, Preserve)
  .setPreference(DoubleIndentConstructorArguments, false)

// Scala linting to help preventing bugs
wartremoverErrors ++= Warts.allBut(
    Wart.Equals,
    Wart.Overloading,
    Wart.DefaultArguments,
    Wart.Throw,
    Wart.Any,
    Wart.ImplicitParameter) ++
    Seq(Wart.custom("com.kinja.warts.StrictTime"))

scalacOptions ++=
  (dependencyClasspath in Compile).value.files.map("-P:wartremover:cp:" + _.toURI.toURL)
