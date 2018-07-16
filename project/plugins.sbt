resolvers := Seq(
	"Kinja Public Group" at sys.env.getOrElse("KINJA_PUBLIC_REPO", "https://kinjajfrog.jfrog.io/kinjajfrog/sbt-virtual"),
	"sonatype-releases" at "https://oss.sonatype.org/content/repositories/releases/"
)

credentials += Credentials(Path.userHome / ".ivy2" / ".kinja-artifactory.credentials")

// Automatic code formatting
addSbtPlugin("org.scalariform" % "sbt-scalariform" % "1.6.0")

// Scalastyle
addSbtPlugin("org.scalastyle" %% "scalastyle-sbt-plugin" % "0.8.0")

// Kinja build plugin
addSbtPlugin("com.kinja.sbtplugins" %% "kinja-build-plugin" % "3.2.1")

// Scala linting plugin
addSbtPlugin("org.wartremover" % "sbt-wartremover" % "2.1.1")
