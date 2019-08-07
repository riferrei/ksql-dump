= KSQL Dump
Simple utility written in Go to dump KSQL streams and tables into a file

== Building
[source,bash]
----
go build -o ksqldump
----

== Using
[source,bash]
----
ksqldump -s <KSQL_SERVER> -f <FILENAME>
----
