FROM openjdk:11.0.5-jre-slim

MAINTAINER Murilo Pereira <murilo@murilopereira.com>

ADD target/uberjar/elasticsearch-stress-0.1.0-SNAPSHOT-standalone.jar /srv

ENTRYPOINT ["java", "-jar", "/srv/elasticsearch-stress-0.1.0-SNAPSHOT-standalone.jar"]
