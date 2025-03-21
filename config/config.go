package config

import (
	"log/slog"
	"os"
	"path"
	"time"

	"github.com/spf13/viper"
)

type Config struct {
	Env                string            `mapstructure:"env"`
	LogLevel           string            `mapstructure:"log_level"`
	LogType            string            `mapstructure:"log_type"`
	ServiceName        string            `mapstructure:"service_name"`
	Port               string            `mapstructure:"port"`
	Version            string            `mapstructure:"version"`
	WorkerSettings     *WorkerConfig     `mapstructure:"worker"`
	CacheSettings      *CacheConfig      `mapstructure:"cache"`
	DbSettings         *DatabaseConfig   `mapstructure:"database"`
	KafkaSettings      *KafkaConfig      `mapstructure:"kafka"`
	S3Settings         *S3Config         `mapstructure:"s3"`
	CrawlerSettings    *CrawlerConfig    `mapstructure:"crawler"`
	TelemetrySettings  *TelemetryConfig  `mapstructure:"telemetry"`
	HttpClientSettings *HttpClientConfig `mapstructure:"http_client"`
}

type WorkerConfig struct {
	WorkersNum     int           `mapstructure:"workers_num"`
	CrawlMechanism int           `mapstructure:"crawl_mechanism"`
	RetryAttempts  int           `mapstructure:"retry_attempts"`
	RetryDelay     time.Duration `mapstructure:"retry_delay"`
	UserAgent      string        `mapstructure:"user_agent"`
}

type CacheConfig struct {
	Servers    []string      `mapstructure:"servers"`
	TtlForPage time.Duration `mapstructure:"ttl_for_page"`
}

type DatabaseConfig struct {
	Host            string        `mapstructure:"host"`
	Port            string        `mapstructure:"port"`
	User            string        `mapstructure:"user"`
	Password        string        `mapstructure:"password"`
	Name            string        `mapstructure:"name"`
	ConnMaxLifetime time.Duration `mapstructure:"conn_max_lifetime"`
	MaxOpenConns    int           `mapstructure:"max_open_conns"`
	MaxIdleConns    int           `mapstructure:"max_idle_conns"`
}

type KafkaConfig struct {
	Producer *ProducerConfig `mapstructure:"producer"`
	Consumer *ConsumerConfig `mapstructure:"consumer"`
}

type ProducerConfig struct {
	Addr                []string      `mapstructure:"addr"`
	WriteTopicName      string        `mapstructure:"write_topic_name"`
	DeadLetterTopicName string        `mapstructure:"dlq_topic_name"`
	MaxAttempts         int           `mapstructure:"max_attempts"`
	BatchSize           int           `mapstructure:"batch_size"`
	BatchTimeout        time.Duration `mapstructure:"batch_timeout"`
	ReadTimeout         time.Duration `mapstructure:"read_timeout"`
	WriteTimeout        time.Duration `mapstructure:"write_timeout"`
	RequiredAsks        int           `mapstructure:"required_acks"`
	Async               bool          `mapstructure:"async"`
}

type ConsumerConfig struct {
	ReadTopicName    string        `mapstructure:"read_topic_name"`
	Brokers          []string      `mapstructure:"brokers"`
	GroupID          string        `mapstructure:"group_id"`
	MaxWait          time.Duration `mapstructure:"max_wait"`
	ReadBatchTimeout time.Duration `mapstructure:"read_batch_timeout"`
	QueueCapacity    int           `mapstructure:"queue_capacity"`
	MaxBytes         int           `mapstructure:"max_bytes"`
	CommitInterval   time.Duration `mapstructure:"commit_interval"`
}

type S3Config struct {
	AwsBaseEndpoint string `mapstructure:"aws_base_endpoint"`
	Region          string `mapstructure:"region"`
	BucketName      string `mapstructure:"bucket_name"`
	KeyPrefix       string `mapstructure:"key_prefix"`
}

type CrawlerConfig struct {
	RequestTimeout   int `mapstructure:"request_timeout"`
	Retries          int `mapstructure:"retries"`
	LastCrawlIndexes int `mapstructure:"last_crawl_indexes"`
}

type TelemetryConfig struct {
	Enabled      bool   `mapstructure:"enabled"`
	CollectorUrl string `mapstructure:"collector_url"`
}

type HttpClientConfig struct {
	RequestTimeout            time.Duration `mapstructure:"request_timeout"`
	MaxIdleConnections        int           `mapstructure:"max_idle_connections"`
	MaxIdleConnectionsPerHost int           `mapstructure:"max_idle_connections_per_host"`
	MaxConnectionsPerHost     int           `mapstructure:"max_connections_per_host"`
	IdleConnectionTimeout     time.Duration `mapstructure:"idle_connection_timeout"`
	TlsHandshakeTimeout       time.Duration `mapstructure:"tls_handshake_timeout"`
	DialTimeout               time.Duration `mapstructure:"dial_timeout"`
	DialKeepAlive             time.Duration `mapstructure:"dial_keep_alive"`
	TlsInsecureSkipVerify     bool          `mapstructure:"tls_insecure_skip_verify"`
}

func MustLoad() *Config {
	viper.AddConfigPath(path.Join("."))
	viper.SetConfigName("config")
	viper.AutomaticEnv()

	err := viper.ReadInConfig()
	if err != nil {
		slog.Error("can't initialize config file.", slog.String("err", err.Error()))
		os.Exit(1)
	}

	var cfg Config
	if err := viper.Unmarshal(&cfg); err != nil {
		slog.Error("error unmarshalling viper config.", slog.String("err", err.Error()))
		os.Exit(1)
	}

	return &cfg
}
