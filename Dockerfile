FROM openjdk:17-alpine
WORKDIR /home/app
COPY build/docker/main/layers/libs /home/app/libs
COPY build/docker/main/layers/classes /home/app/classes
COPY build/docker/main/layers/resources /home/app/resources
COPY build/docker/main/layers/application.jar /home/app/application.jar
COPY ui/build ./ui/build
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "/home/app/application.jar"]
