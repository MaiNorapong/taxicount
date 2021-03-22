export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export PATH=$PATH:$JAVA_HOME/bin:$JAVA_HOME/jre/bin
export HADOOP_CLASSPATH=$JAVA_HOME/lib/tools.jar
export CLASSPATH=./
export CLASSPATH=$CLASSPATH:`hadoop classpath`:.:
