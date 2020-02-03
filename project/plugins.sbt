resolvers := Seq(
	"Kinja Public" at sys.env.getOrElse("KINJA_PUBLIC_REPO", "https://kinjajfrog.jfrog.io/kinjajfrog/sbt-virtual"),
	"sonatype-releases" at "https://oss.sonatype.org/content/repositories/releases/"
)

credentials += Credentials(Path.userHome / ".ivy2" / ".kinja-artifactory.credentials")

// Automatic code formatting
addSbtPlugin("org.scalariform" % "sbt-scalariform" % "1.8.2")

// Scalastyle
addSbtPlugin("org.scalastyle" %% "scalastyle-sbt-plugin" % "1.0.0")

// Kinja build plugin
addSbtPlugin("com.kinja.sbtplugins" %% "kinja-build-plugin" % "3.2.5")

// Scala linting plugin
addSbtPlugin("org.wartremover" % "sbt-wartremover" % "2.4.2")
