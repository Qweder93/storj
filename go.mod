module storj.io/storj

go 1.13

// force specific versions for minio
require (
	github.com/btcsuite/btcutil v0.0.0-20180706230648-ab6388e0c60a
	github.com/garyburd/redigo v1.0.1-0.20170216214944-0d253a66e6e1 // indirect
	github.com/graphql-go/graphql v0.7.9-0.20190403165646-199d20bbfed7
	github.com/minio/minio v0.0.0-20180508161510-54cd29b51c38
	github.com/segmentio/go-prompt v1.2.1-0.20161017233205-f0d19b6901ad
)

exclude gopkg.in/olivere/elastic.v5 v5.0.72 // buggy import, see https://github.com/olivere/elastic/pull/869

replace google.golang.org/grpc => github.com/storj/grpc-go v1.23.1-0.20190918084400-1c4561bf5127

require (
	github.com/Shopify/go-lua v0.0.0-20181106184032-48449c60c0a9
	github.com/alessio/shellescape v0.0.0-20190409004728-b115ca0f9053
	github.com/alicebob/miniredis/v2 v2.11.1
	github.com/armon/go-metrics v0.0.0-20180917152333-f0300d1749da // indirect
	github.com/blang/semver v3.5.1+incompatible
	github.com/boltdb/bolt v1.3.1
	github.com/cheggaaa/pb v1.0.5-0.20160713104425-73ae1d68fe0b // indirect
	github.com/cheggaaa/pb/v3 v3.0.1
	github.com/djherbis/atime v1.0.0 // indirect
	github.com/dustin/go-humanize v1.0.0 // indirect
	github.com/eclipse/paho.mqtt.golang v1.1.1 // indirect
	github.com/elazarl/go-bindata-assetfs v1.0.0 // indirect
	github.com/fatih/color v1.7.0
	github.com/fatih/structs v1.0.0 // indirect
	github.com/go-ini/ini v1.38.2 // indirect
	github.com/go-redis/redis v6.14.1+incompatible
	github.com/gogo/protobuf v1.2.1
	github.com/golang-migrate/migrate/v4 v4.7.0
	github.com/golang/protobuf v1.3.2
	github.com/gomodule/redigo v2.0.0+incompatible // indirect
	github.com/google/go-cmp v0.3.0
	github.com/gopherjs/gopherjs v0.0.0-20181103185306-d547d1d9531e // indirect
	github.com/gorilla/handlers v1.4.0 // indirect
	github.com/gorilla/mux v1.7.1
	github.com/gorilla/rpc v1.1.0 // indirect
	github.com/gorilla/schema v1.1.0
	github.com/hashicorp/go-immutable-radix v1.0.0 // indirect
	github.com/hashicorp/go-msgpack v0.5.3 // indirect
	github.com/hashicorp/raft v1.0.0 // indirect
	github.com/howeyc/gopass v0.0.0-20170109162249-bf9dde6d0d2c // indirect
	github.com/inconshreveable/go-update v0.0.0-20160112193335-8152e7eb6ccf // indirect
	github.com/jackc/pgx v3.2.0+incompatible
	github.com/jtolds/gls v4.2.1+incompatible // indirect
	github.com/jtolds/go-luar v0.0.0-20170419063437-0786921db8c0
	github.com/jtolds/monkit-hw/v2 v2.0.0-20191108235325-141a0da276b3
	github.com/jtolds/tracetagger/v2 v2.0.0-rc0
	github.com/klauspost/cpuid v0.0.0-20180405133222-e7e905edc00e // indirect
	github.com/klauspost/reedsolomon v0.0.0-20180704173009-925cb01d6510 // indirect
	github.com/lib/pq v1.3.0
	github.com/loov/hrtime v0.0.0-20181214195526-37a208e8344e
	github.com/loov/plot v0.0.0-20180510142208-e59891ae1271
	github.com/mattn/go-isatty v0.0.9 // indirect
	github.com/mattn/go-sqlite3 v2.0.2+incompatible
	github.com/minio/cli v1.3.0
	github.com/minio/dsync v0.0.0-20180124070302-439a0961af70 // indirect
	github.com/minio/highwayhash v0.0.0-20180501080913-85fc8a2dacad // indirect
	github.com/minio/lsync v0.0.0-20180328070428-f332c3883f63 // indirect
	github.com/minio/mc v0.0.0-20180926130011-a215fbb71884 // indirect
	github.com/minio/minio-go v6.0.3+incompatible
	github.com/minio/sio v0.0.0-20180327104954-6a41828a60f0 // indirect
	github.com/nats-io/gnatsd v1.3.0 // indirect
	github.com/nats-io/go-nats v1.6.0 // indirect
	github.com/nats-io/go-nats-streaming v0.4.2 // indirect
	github.com/nats-io/nats v1.6.0 // indirect
	github.com/nats-io/nats-streaming-server v0.12.2 // indirect
	github.com/nats-io/nuid v1.0.0 // indirect
	github.com/nsf/jsondiff v0.0.0-20160203110537-7de28ed2b6e3
	github.com/nsf/termbox-go v0.0.0-20190121233118-02980233997d
	github.com/pascaldekloe/goe v0.0.0-20180627143212-57f6aae5913c // indirect
	github.com/pkg/profile v1.2.1 // indirect
	github.com/prometheus/procfs v0.0.0-20190517135640-51af30a78b0e // indirect
	github.com/rs/cors v1.5.0 // indirect
	github.com/shopspring/decimal v0.0.0-20200105231215-408a2507e114
	github.com/skyrings/skyring-common v0.0.0-20160929130248-d1c0bb1cbd5e
	github.com/smartystreets/assertions v0.0.0-20180820201707-7c9eb446e3cf // indirect
	github.com/smartystreets/goconvey v0.0.0-20180222194500-ef6db91d284a // indirect
	github.com/spacemonkeygo/monkit/v3 v3.0.1
	github.com/spf13/cast v1.3.0
	github.com/spf13/cobra v0.0.5
	github.com/spf13/pflag v1.0.3
	github.com/spf13/viper v1.4.0
	github.com/streadway/amqp v0.0.0-20180806233856-70e15c650864 // indirect
	github.com/stretchr/testify v1.4.0
	github.com/stripe/stripe-go v63.1.1+incompatible
	github.com/tidwall/gjson v1.1.3 // indirect
	github.com/tidwall/match v0.0.0-20171002075945-1731857f09b1 // indirect
	github.com/vivint/infectious v0.0.0-20190108171102-2455b059135b
	github.com/zeebo/admission/v2 v2.0.0-20191108235419-c7982d45d29a
	github.com/zeebo/errs v1.2.2
	github.com/zeebo/structs v1.0.2
	go.uber.org/zap v1.10.0
	golang.org/x/crypto v0.0.0-20191227163750-53104e6ec876
	golang.org/x/sync v0.0.0-20190911185100-cd5d95a43a6e
	golang.org/x/sys v0.0.0-20200107144601-ef85f5a75ddf
	golang.org/x/time v0.0.0-20191024005414-555d28b269f0
	google.golang.org/appengine v1.6.0 // indirect
	google.golang.org/grpc v1.23.1
	gopkg.in/Shopify/sarama.v1 v1.18.0 // indirect
	gopkg.in/cheggaaa/pb.v1 v1.0.25 // indirect
	gopkg.in/ini.v1 v1.38.2 // indirect
	gopkg.in/olivere/elastic.v5 v5.0.76 // indirect
	gopkg.in/yaml.v2 v2.2.4
	storj.io/common v0.0.0-20200210174421-a3a30fc4862d
	storj.io/drpc v0.0.8
	storj.io/uplink v0.0.0-20200131143957-f3b6350b4add
)
