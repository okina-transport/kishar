FROM eclipse-temurin:21.0.5_11-jre

ARG JAR_FILE
COPY ${JAR_FILE} kishar.jar


EXPOSE 8888
CMD java -XX:+ExitOnOutOfMemoryError $JAVA_OPTIONS -jar /kishar.jar