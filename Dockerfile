FROM scratch

ADD https://github.com/hora-prediction/admx/releases/download/v0.0.1/admx-linux-amd64 /admx

EXPOSE 8081
CMD ["/admx"]
