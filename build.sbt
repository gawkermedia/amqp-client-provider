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

val CI_BUILD = System.getProperty("JENKINS_BUILD") == "true"
val nexusUrl = System.getenv("NEXUS_URL")
val nexusSnapShotPath = System.getenv.getOrDefault("NEXUS_SNAPSHOT_PATH", "/content/repositories/snapshots/")
val nexusReleasesPath = System.getenv.getOrDefault("NEXUS_RELEASES_PATH", "/content/repositories/releases/")
val nexusPublicGroupPath = System.getenv.getOrDefault("NEXUS_PUBLIC_GROUP_PATH", "/content/groups/public/")

credentials += Credentials(Path.userHome / ".ivy2" / ".credentials")

publishTo <<= (version)(version =>
    if (CI_BUILD) {
        if (version endsWith "SNAPSHOT") Some("Snapshots" at nexusUrl + nexusSnapShotPath)
        else                             Some("Releases" at nexusUrl + nexusReleasesPath)
    } else {
        None
    }
)

resolvers += "Public Group" at nexusUrl + nexusPublicGroupPath

enablePlugins(GitVersioning)

git.gitTagToVersionNumber := { tag: String =>
    Some(tag.replace("release/", ""))
}