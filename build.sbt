name := "amqp-client-provider"

organization := "com.kinja"

scalaVersion := "2.10.4"

crossScalaVersions := Seq("2.10.4", "2.11.6")

scalacOptions  ++= Seq("-feature", "-language:postfixOps")

shellPrompt in ThisBuild := { state => Project.extract(state).currentRef.project + "> " }

incOptions := incOptions.value.withNameHashing(true)

val akkaVersion = "2.3.12"

libraryDependencies ++= Seq(
    "com.kinja" %% "amqp-client" % "1.5.1",
    "com.typesafe.play" %% "play-json" % "2.3.4",
    "com.typesafe.akka" %% "akka-actor" % akkaVersion % "provided",
    "ch.qos.logback" % "logback-classic" % "1.0.0" % "provided",
    // Test dependencies
    "org.specs2" %% "specs2-core" % "3.6" % "test",
    "org.specs2" %% "specs2-junit" % "3.6" % "test",
    "org.specs2" %% "specs2-mock" % "3.6" % "test",
    "org.specs2" %% "specs2-scalacheck" % "3.6" % "test",
    "com.h2database" % "h2" % "1.4.187" % "test"
)

// External plugins
scalariformSettings

// code formatting
ScalariformKeys.preferences := scalariform.formatter.preferences.FormattingPreferences()
    .setPreference(scalariform.formatter.preferences.IndentWithTabs, true)
    .setPreference(scalariform.formatter.preferences.PreserveDanglingCloseParenthesis, true)

def getEnvOrDefault(key: String, default: String): String = {
    if (System.getenv().containsKey(key)) {
        System.getenv(key)
    } else {
        default
    }
}

val CI_BUILD = System.getProperty("JENKINS_BUILD") == "true"
val nexusUrl = System.getenv("NEXUS_URL")
val nexusSnapShotPath = getEnvOrDefault("NEXUS_SNAPSHOT_PATH", "/content/repositories/snapshots/")
val nexusReleasesPath = getEnvOrDefault("NEXUS_RELEASES_PATH", "/content/repositories/releases/")
val nexusPublicGroupPath = getEnvOrDefault("NEXUS_PUBLIC_GROUP_PATH", "/content/groups/public/")

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