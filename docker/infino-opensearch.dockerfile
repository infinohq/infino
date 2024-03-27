# Builder stage - for building the plugin
ARG BUILDER_IMAGE=focal-curl
ARG OPENSEARCH_VERSION=2.11.1

FROM buildpack-deps:$BUILDER_IMAGE as builder

ARG OPENSEARCH_VERSION

# Update and install necessary packages
RUN apt-get update && export DEBIAN_FRONTEND=noninteractive && \
    apt-get install -y zip unzip


# Download and install SDKMAN and use it to install Java 17.0.9-amzn
SHELL ["/bin/bash", "-c"]
RUN curl -s "https://get.sdkman.io" | bash && \
    source "$HOME/.sdkman/bin/sdkman-init.sh" && \
    sdk install java 17.0.9-amzn

# Set current working directory for infino-opensearch-plugin
WORKDIR /usr/src/infino-opensearch-plugin

# Copy the source code to the container
COPY plugins/infino-opensearch-plugin .

# Build the plugin
## TODO: Need to cleanup this env var mess. Using
## ENV instruction to set JAVA_HOME to $HOME/<something>
## doesn't work as $HOME is treated as a build arg in
## the dockerfile and not as an env var in the image.
## Hence the mess.
RUN JAVA_HOME=$HOME/.sdkman/candidates/java/current \
    PATH=$PATH:$JAVA_HOME/bin \
    ./gradlew clean && \
    JAVA_HOME=$HOME/.sdkman/candidates/java/current \
    PATH=$PATH:$JAVA_HOME/bin \
    ./gradlew assemble -Dopensearch.version=$OPENSEARCH_VERSION

# Use the standard OpenSearch image as base and install the infino plugin in it.
FROM opensearchproject/opensearch:$OPENSEARCH_VERSION

USER root

## Add Linux tools for debugging
### The container doesn't have tools like vim, find, less
### etc. Please add them here and build the container again.
RUN yum install -y vim

USER opensearch

# Set the curent working directory to /usr/share/opensearch
WORKDIR /usr/share/opensearch

# Copy the infino plugin zip file from the builder to the container
COPY --from=builder /usr/src/infino-opensearch-plugin/build/distributions/infino-opensearch-plugin-*.zip .

# Update the java security policy to allow the plugin to load and run
RUN printf \
   'grant {\n    // Add permissions to allow Infino OpenSearch Plugin to access Infino using OpenSearch threadpool\n\
    permission org.opensearch.secure_sm.ThreadPermission "modifyArbitraryThread";\n\
    permission java.net.URLPermission "http://*:*/-", "*:*";\n\
    permission java.lang.RuntimePermission "accessUserInformation";\n};' \
   >> /usr/share/opensearch/jdk/conf/security/java.policy

# Install the infino plugin
## Note: Based on the opensearch version, it's a bit windy to get the plugin version and then
## use it to get the plugin zip file name. Since we know that there is only one .zip file
## matching the "infino-opensearch-plugin-*.zip" pattern, we can use the basename command to
## get the exact file name from the glob pattern.
RUN echo 'y' | bin/opensearch-plugin install file:///usr/share/opensearch/$(basename infino-opensearch-plugin-*.zip)

# Expose the default ports that the base opensearch image exposes
## opensearch service -> 9200 for HTTP and 9300 for internal transport
## performance analyzer -> 9600 for the agent and 9650 for the root cause analysis component
EXPOSE 9200 9300 9600 9650

# Start OpenSearch when the container starts
## Not specifying any ENTRYPOINT or CMD so that the settings from the base image kicks in