FROM scratch

ADD admx /admx

EXPOSE 8081
CMD ["/admx"]
