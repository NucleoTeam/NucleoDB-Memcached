FROM docker.io/openjdk:20-slim-bullseye as builder

ADD ./ /

RUN cd /;./gradlew shadowJar
RUN cd /;ls /build/libs/

FROM docker.io/openjdk:22-slim-bullseye

COPY --from=builder /build/libs/Shadow-*.jar /

ENV PORT=11211
ENV KAFKA_SERVERS=kafka:9092

CMD java -jar  /Shadow-*.jar
