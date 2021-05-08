# Java spark 
## Hello world example using spark 3.1.1 windows



When try to build the spak application in Intelij with java 11, I faced lot of issues,
I workd and find the list of issues and fixed. This project help to run spark application without any issue.

- Use gradle build file
- Spark version 3.1.1
- Windows Operating system with local file system



## Tech

spark example uses a number of open source projects to work properly:

- [spark] - Open source big data framework
- [intellij Community edition] - awesome java IDE editor
- [JDK] - Open source JDK.


## Installation

Spark example requires spark 3.1.1 to run.

Install the dependencies and devDependencies and start the spark using pyspark or spark-shell.

#### Building

For runing the sample spark:

```sh
spark-submit run-example org.apache.spark.examples.SparkPi
```

```sh
21/04/30 20:04:27 INFO DAGScheduler: Job 0 finished: reduce at SparkPi.scala:38, took 0.419737 s
Pi is roughly 3.143955719778599
21/04/30 20:04:27 INFO SparkUI: Stopped Spark web UI at http://localhost:4040
```

Build the application in gradle

```sh
./gradlew clean build
```

run the spark example:

```sh
java -jar build\libs\spark-hello-1.0-SNAPSHOT.jar SparkHelloWorld
```

output
```sh
21/05/01 05:51:26 INFO DAGScheduler: Job 1 finished: collect at SparkHelloWorld.java:46, took 0.198112 s
preferences: 1
cancel: 1
its: 1
than: 1
10.: 1
"Find: 1
content: 1
changed.: 1
shortcut: 1
(with: 1
1.: 1
Session: 1
Enhancements: 1
folder: 1
menu: 1
dragging: 1
3.: 1
for: 1
makes: 1
network: 1
&: 1
9.: 1
empty: 1
file: 3
feature.: 1
the: 1
entry: 1
Projects": 1
2.: 1
v4.2.0: 1
not: 3
Notepad++: 2
mute: 1
issue: 2
Add: 2
provided: 1
11.: 1
all: 1
plugin): 1
into: 1
monitor: 1
regression.: 1
v7.9.4: 1
picker: 1
path: 1
Scintilla: 1
v4.4.6.: 1
an: 1
8.: 1
4.      Fix: 1
path): 1
turning: 1
save: 1
bug-fixes:: 1
5.: 1
is: 1
Fix: 6
7.: 1
on: 1
while: 1
issue.: 2
in: 4
appending: 1
Make: 1
lost: 1
workspace: 1
JSON: 1
from: 1
Upgrade: 1
bug: 1
64: 1
2nd: 1
DPI: 1
sounds: 1
after: 1
directory: 1
project: 1
(UNC: 1
because: 1
extension: 1
name: 2
a: 1
scaling: 1
state: 1
invalid.: 1
Manager: 1
to: 3
: 9
more: 1
auto-indent: 1
option: 1
launched: 1
broken: 1
changes: 1
characters: 1
working: 1
modified: 1
6.: 1
being: 2
brace.: 1
dialog.: 3
```

## License

MIT

**Free Software, Hell Yeah!**
