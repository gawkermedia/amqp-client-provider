resolvers += "Gawker Public Group" at "https://nexus.kinja-ops.com/nexus/content/groups/public"

credentials += Credentials(Path.userHome / ".ivy2" / ".credentials")

addSbtPlugin("org.scalariform" % "sbt-scalariform" % "1.4.0")
