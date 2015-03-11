#!/bin/sh

JVM_ARGUMENTS="\
-Dnetworkaddress.cache.ttl=-1 \
-Djava.net.preferIPv4Stack=true \
-server \
-XX:PermSize=64m \
-XX:MaxPermSize=128m \
-Xms256m \
-Xmx1989m \
-XX:+UseConcMarkSweepGC \
-XX:+CMSClassUnloadingEnabled \
-Dcom.sun.management.jmxremote \
-Dcom.sun.management.jmxremote.port=9876 \
-Dcom.sun.management.jmxremote.local.only=false \
-Dcom.sun.management.jmxremote.authenticate=false \
-Dcom.sun.management.jmxremote.ssl=false"

join() {
    local delim=$1      # join delimiter
    shift
    local oldIFS=$IFS   # save IFS, the field separator
    IFS=$delim
    local result="$*"
    IFS=$oldIFS   # restore IFS
    echo $result
}

# Spring Boot needs comma-separated delimiter.
ARGS=$(join , ${@})
echo $ARGS

COMMAND="mvn -Dmaven.test.skip clean spring-boot:run -Drun.jvmArguments=\""${JVM_ARGUMENTS}"\" -Drun.arguments=\""${ARGS}"\""

eval $COMMAND
