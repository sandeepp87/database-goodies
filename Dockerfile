FROM openjdk:8-jre-alpine

ENV TZ=America/Los_Angeles
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone
ENV JAVA_TOOL_OPTIONS="-Dfile.encoding=UTF8 -Djdk.http.auth.tunneling.disabledSchemes='' -Djava.security.egd=file:/dev/./urandom"

#copy jar
RUN mkdir -p /app
WORKDIR /app
COPY database-goodies-1.3-SNAPSHOT.jar app.jar
COPY migrate-local.properties local.properties
COPY server-ca.pem server-ca.pem
COPY client-cert.pem client-cert.pem
COPY client-key.pk8 client-key.pk8


CMD ["java", \
     "-Dlocal.properties=/app/local.properties", \
     "-jar","/app/app.jar", \
     "com.github.susom.dbgoodies.etl.EtlCopyTables"]
