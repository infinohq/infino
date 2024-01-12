ARG IMAGE_VARIANT=focal-curl
FROM buildpack-deps:$IMAGE_VARIANT

ARG USERNAME=infino
ARG USER_UID=1000
ARG USER_GID=$USER_UID
ARG USER_HOME=/home/$USERNAME

# Execute commands as root
USER root

# Update and install necessary packages
RUN apt-get update && export DEBIAN_FRONTEND=noninteractive && \
    apt-get install -y build-essential zip unzip curl sudo && \
    # we need pkg-config and libssl-dev for cargo to build infino (otherwise cargo complains)
    apt-get install -y libssl-dev pkg-config && \
    # Cleanup after the installs to remove the apt cache and any temporary files
    apt-get autoremove -y && \
    apt-get clean -y && \
    rm -fr /var/lib/apt/lists/* && \
    # Create user $USERNAME with UID $USER_UID and in group $USERNAME with GID $USER_GID
    groupadd -g $USER_GID $USERNAME && \
    useradd -u $USER_UID -g $USER_GID -d $USER_HOME $USERNAME -s /bin/bash && \
    mkdir -p $USER_HOME && \
    chown -R $USER_UID:$USER_GID $USER_HOME && \
    # Add sudo support for user 'infino'
    echo $USERNAME ALL=\(root\) NOPASSWD:ALL > /etc/sudoers.d/$USERNAME && \
    chmod 0440 /etc/sudoers.d/$USERNAME

# Switch to user 'infino' and execute user-specific commands
## We create a non-root user as it is recommended to run infino,
## the infino-opensearch-plugin and OpenSearch itself as a non-root user.
USER $USERNAME

# Download and install SDKMAN and use it to install Java 17.0.9-amzn
SHELL ["/bin/bash", "-c"]
RUN curl -s "https://get.sdkman.io" | bash && \
    source "$USER_HOME/.sdkman/bin/sdkman-init.sh" && \
    sdk install java 17.0.9-amzn
