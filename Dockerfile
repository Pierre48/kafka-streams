FROM openjdk:15
COPY ./out/production/DockerJavaApp/ /tmp
WORKDIR /tmp
ENTRYPOINT ["java","MergeTopics"]