FROM eclipse-temurin:21-jre-alpine
VOLUME /tmp
ARG JAR_FILE=target/shop-api-0.0.1-SNAPSHOT.jar
COPY ${JAR_FILE} app.jar
EXPOSE 8051
ENTRYPOINT ["java","-jar","/app.jar"]