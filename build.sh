sbt/sbt -Dhadoop.version=2.2.0 clean
sbt/sbt -Dhadoop.version=2.2.0 assembly
sbt/sbt -Dhadoop.version=2.2.0 gen-idea
sbt/sbt -Dhadoop.version=2.2.0 publish-local
./make-distribution.sh -Phadoop.version=2.2.0
#cd ~/sqltest
#sbt package
