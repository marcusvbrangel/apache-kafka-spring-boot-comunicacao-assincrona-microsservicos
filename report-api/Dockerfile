FROM eclipse-temurin:21-jre-alpine
VOLUME /tmp
ARG JAR_FILE=target/report-api-0.0.1-SNAPSHOT.jar
COPY ${JAR_FILE} app.jar
EXPOSE 8053
ENTRYPOINT ["java","-jar","/app.jar"]