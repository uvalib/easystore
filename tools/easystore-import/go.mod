module github.com/uvalib/easystore-import

go 1.24.0

toolchain go1.24.3

require github.com/uvalib/easystore/uvaeasystore v0.0.0

replace github.com/uvalib/easystore/uvaeasystore => ../../uvaeasystore

require (
	github.com/aws/aws-sdk-go-v2 v1.39.5 // indirect
	github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream v1.7.2 // indirect
	github.com/aws/aws-sdk-go-v2/config v1.31.16 // indirect
	github.com/aws/aws-sdk-go-v2/credentials v1.18.20 // indirect
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.18.12 // indirect
	github.com/aws/aws-sdk-go-v2/feature/s3/manager v1.20.2 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.4.12 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.7.12 // indirect
	github.com/aws/aws-sdk-go-v2/internal/ini v1.8.4 // indirect
	github.com/aws/aws-sdk-go-v2/internal/v4a v1.4.12 // indirect
	github.com/aws/aws-sdk-go-v2/service/cloudwatchevents v1.32.10 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.13.2 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/checksum v1.9.3 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.13.12 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/s3shared v1.19.12 // indirect
	github.com/aws/aws-sdk-go-v2/service/s3 v1.89.1 // indirect
	github.com/aws/aws-sdk-go-v2/service/sso v1.30.0 // indirect
	github.com/aws/aws-sdk-go-v2/service/ssooidc v1.35.4 // indirect
	github.com/aws/aws-sdk-go-v2/service/sts v1.39.0 // indirect
	github.com/aws/smithy-go v1.23.2 // indirect
	github.com/lib/pq v1.10.9 // indirect
	github.com/rs/xid v1.6.0 // indirect
	github.com/uvalib/librabus-sdk/uvalibrabus v0.0.0-20250801130056-157231a1fcac // indirect
	golang.org/x/exp v0.0.0-20251023183803-a4bb9ffd2546 // indirect
)
