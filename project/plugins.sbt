resolvers += "Gawker Public Group" at "https://nexus.kinja-ops.com/nexus/content/groups/public"

credentials += Credentials(Path.userHome / ".ivy2" / ".credentials")

addSbtPlugin("com.typesafe.sbt" % "sbt-scalariform" % "1.2.0")
