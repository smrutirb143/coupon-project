FROM amd64/golang:1.21.1 as build
ARG APP=coupon-management

# Setting ENv
ENV APP ${APP}
ENV GO111MODULE=on \
    CGO_ENABLED=0  \
    GOOS=linux   \
    GOARCH="amd64" \
    TZ=Asia/Kolkata
 
RUN mkdir -p /opt/product1/
ADD . /opt/product1/
WORKDIR /opt/product1/coupon-management/
RUN go mod download
RUN go build -o coupon-api/couponApi coupon-api/main.go
RUN mkdir /var/run/


CMD ["oupon-api/couponApi", "-config=/etc/coupon_loader_config.yaml"]