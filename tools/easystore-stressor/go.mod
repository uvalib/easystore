module github.com/uvalib/easystore-stressor

go 1.25.4

require github.com/uvalib/easystore/uvaeasystore v0.0.0

require (
	github.com/aws/aws-sdk-go-v2 v1.41.0 // indirect
	github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream v1.7.4 // indirect
	github.com/aws/aws-sdk-go-v2/config v1.32.4 // indirect
	github.com/aws/aws-sdk-go-v2/credentials v1.19.4 // indirect
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.18.16 // indirect
	github.com/aws/aws-sdk-go-v2/feature/s3/manager v1.20.14 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.4.16 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.7.16 // indirect
	github.com/aws/aws-sdk-go-v2/internal/ini v1.8.4 // indirect
	github.com/aws/aws-sdk-go-v2/internal/v4a v1.4.16 // indirect
	github.com/aws/aws-sdk-go-v2/service/cloudwatchevents v1.32.17 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.13.4 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/checksum v1.9.7 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.13.16 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/s3shared v1.19.16 // indirect
	github.com/aws/aws-sdk-go-v2/service/s3 v1.93.1 // indirect
	github.com/aws/aws-sdk-go-v2/service/signin v1.0.4 // indirect
	github.com/aws/aws-sdk-go-v2/service/sso v1.30.7 // indirect
	github.com/aws/aws-sdk-go-v2/service/ssooidc v1.35.12 // indirect
	github.com/aws/aws-sdk-go-v2/service/sts v1.41.4 // indirect
	github.com/aws/smithy-go v1.24.0 // indirect
	github.com/lib/pq v1.10.9 // indirect
	github.com/rs/xid v1.6.0 // indirect
	github.com/uvalib/librabus-sdk/uvalibrabus v0.0.0-20250801130056-157231a1fcac // indirect
	golang.org/x/exp v0.0.0-20251125195548-87e1e737ad39 // indirect
)

replace github.com/uvalib/easystore/uvaeasystore => ../../uvaeasystore
