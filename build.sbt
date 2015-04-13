name := "amqp-client-provider"

organization := "com.kinja"

version := "0.0.3" + {if (System.getProperty("JENKINS_BUILD") == null) "-SNAPSHOT" else ""}

scalaVersion := "2.10.3"

scalacOptions  ++= Seq("-feature", "-language:postfixOps")

shellPrompt in ThisBuild := { state => Project.extract(state).currentRef.project + "> " }

libraryDependencies ++= Seq(
    "com.kinja" %% "amqp-client" % "1.4.1-SNAPSHOT",
    "com.typesafe.play" %% "play-json" % "2.3.4"
)

resolvers += "Gawker Public Group" at "https://nexus.kinja-ops.com/nexus/content/groups/public/"

credentials += Credentials(Path.userHome / ".ivy2" / ".credentials")

publishTo <<= (version)(version =>
    if (version endsWith "SNAPSHOT") Some("Gawker Snapshots" at "https://nexus.kinja-ops.com/nexus/content/repositories/snapshots/")
    else                             Some("Gawker Releases" at "https://nexus.kinja-ops.com/nexus/content/repositories/releases/")
)

// External plugins
scalariformSettings

// code formatting
ScalariformKeys.preferences := scalariform.formatter.preferences.FormattingPreferences().
    setPreference(scalariform.formatter.preferences.IndentWithTabs, true)