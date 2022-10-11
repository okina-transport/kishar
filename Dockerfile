FROM openjdk:11-jre

ARG JAR_FILE
COPY ${JAR_FILE} kishar.jar


EXPOSE 8888
CMD java $JAVA_OPTIONS -jar /kishar.jar