FROM scratch

ADD config.toml /config.toml
ADD admx /admx

EXPOSE 8081
CMD ["/admx"]
