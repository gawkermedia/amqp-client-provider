name := "amqp-client-provider"

organization := "com.kinja"

scalaVersion := "2.10.4"

scalacOptions  ++= Seq("-feature", "-language:postfixOps")

shellPrompt in ThisBuild := { state => Project.extract(state).currentRef.project + "> " }

incOptions := incOptions.value.withNameHashing(true)

libraryDependencies ++= Seq(
    "com.kinja" %% "amqp-client" % "1.5.0",
    "com.typesafe.play" %% "play-json" % "2.3.4",
    "com.typesafe.slick" %% "slick" % "1.0.1"
)

// External plugins
scalariformSettings

// code formatting
ScalariformKeys.preferences := scalariform.formatter.preferences.FormattingPreferences()
    .setPreference(scalariform.formatter.preferences.IndentWithTabs, true)
    .setPreference(scalariform.formatter.preferences.PreserveDanglingCloseParenthesis, true)

enablePlugins(GitVersioning)

git.gitTagToVersionNumber := { tag: String =>
    Some(tag.replace("release/", ""))
}