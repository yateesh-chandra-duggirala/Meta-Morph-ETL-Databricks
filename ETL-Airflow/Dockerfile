FROM quay.io/astronomer/astro-runtime:12.7.1

USER root

# Install OpenJDK-17
RUN apt update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get install -y ant && \
    apt-get clean;

# Set JAVA_HOME
# ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-amd64/
# RUN export JAVA_HOME
ENV JAVA_HOME="/usr/lib/jvm/java-17-openjdk-amd64"
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER astro