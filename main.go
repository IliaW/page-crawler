package main

import (
	"context"
	"crypto/tls"
	"database/sql"
	"fmt"
	"log"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"

	_ "net/http/pprof"

	"github.com/IliaW/page-crawler/config"
	"github.com/IliaW/page-crawler/internal/aws_s3"
	"github.com/IliaW/page-crawler/internal/broker"
	cacheClient "github.com/IliaW/page-crawler/internal/cache"
	"github.com/IliaW/page-crawler/internal/crawler"
	"github.com/IliaW/page-crawler/internal/model"
	"github.com/IliaW/page-crawler/internal/persistence"
	"github.com/IliaW/page-crawler/internal/telemetry"
	"github.com/IliaW/page-crawler/internal/worker"
	_ "github.com/lib/pq"
	"github.com/lmittmann/tint"
)

var (
	cfg          *config.Config
	db           *sql.DB
	s3           aws_s3.BucketClient
	cache        cacheClient.CachedClient
	metadataRepo persistence.MetadataStorage
	crawl        *crawler.CommonCrawlerService
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cfg = config.MustLoad()
	setupLogger()
	metrics := telemetry.SetupMetrics(context.Background(), cfg)
	defer metrics.Close()
	db = setupDatabase()
	defer closeDatabase()
	s3 = aws_s3.NewS3BucketClient(cfg)
	cache = cacheClient.NewMemcachedClient(cfg.CacheSettings)
	defer cache.Close()
	crawl = crawler.NewCrawlService(cfg.CrawlerSettings)
	metadataRepo = persistence.NewMetadataRepository(db)
	crawlMechanism := model.CrawlMechanism(cfg.WorkerSettings.CrawlMechanism)
	kafkaDLQ := broker.NewKafkaDLQ(cfg.ServiceName, cfg.KafkaSettings.Producer)
	httpTransport := getHttpTransport()
	slog.Info("starting application on port "+cfg.Port, slog.String("env", cfg.Env),
		slog.String("crawl mechanism", crawlMechanism.String()))

	threadNum := parallelWorkers()
	urlChan := make(chan []byte, threadNum*2)
	processorChan := make(chan *model.ProcessorTask, threadNum*2)

	kafkaWg := &sync.WaitGroup{}
	kafkaWg.Add(1)
	kafkaConsumer := broker.NewKafkaConsumer(urlChan, metrics.KafkaConsumerMetrics,
		cfg.KafkaSettings.Consumer, kafkaWg)
	go kafkaConsumer.Run(ctx)

	workerWg := &sync.WaitGroup{}
	crawlWorker := &worker.CrawlWorker{
		UrlChan:        urlChan,
		ProcessorChan:  processorChan,
		Crawl:          crawl,
		Cfg:            cfg,
		Db:             metadataRepo,
		S3:             s3,
		Cache:          cache,
		Wg:             workerWg,
		CrawlMechanism: crawlMechanism,
		KafkaDLQ:       kafkaDLQ,
		Metrics:        metrics.AppMetrics,
		HttpTransport:  httpTransport,
	}

	for i := 0; i < threadNum; i++ {
		workerWg.Add(1)
		go crawlWorker.Run()
	}

	kafkaWg.Add(1)
	kafkaProducer := broker.NewKafkaProducer(processorChan, metrics.KafkaProducerMetrics,
		cfg.KafkaSettings.Producer, kafkaWg)
	go kafkaProducer.Run()

	go healthCheckHandler()

	// Graceful shutdown.
	// 1. Stop Kafka Consumer by system call. Close urlChan
	// 2. Wait till all Workers processed all messages from urlChan. Close processorChan
	// 3. Wait till Producer process all messages from processorChan and write to Kafka. Stop Kafka Producer
	// 4. Close database and memcached connections
	<-ctx.Done()
	slog.Info("stopping server...")
	workerWg.Wait()
	close(processorChan)
	slog.Info("close processorChan.")
	kafkaWg.Wait()
	slog.Info("server stopped.")
}

func setupLogger() *slog.Logger {
	envLogLevel := strings.ToLower(cfg.LogLevel)
	var slogLevel slog.Level
	err := slogLevel.UnmarshalText([]byte(envLogLevel))
	if err != nil {
		log.Printf("encountenred log level: '%s'. The package does not support custom log levels", envLogLevel)
		slogLevel = slog.LevelDebug
	}
	log.Printf("slog level overwritten to '%v'", slogLevel)
	slog.SetLogLoggerLevel(slogLevel)

	replaceAttrs := func(groups []string, a slog.Attr) slog.Attr {
		if a.Key == slog.SourceKey {
			source := a.Value.Any().(*slog.Source)
			source.File = filepath.Base(source.File)
		}
		return a
	}

	var logger *slog.Logger
	if strings.ToLower(cfg.LogType) == "json" {
		logger = slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
			AddSource:   true,
			Level:       slogLevel,
			ReplaceAttr: replaceAttrs}))
	} else {
		logger = slog.New(tint.NewHandler(os.Stdout, &tint.Options{
			AddSource:   true,
			Level:       slogLevel,
			ReplaceAttr: replaceAttrs,
			NoColor: func() bool {
				if cfg.Env == "local" {
					return false
				}
				return true
			}()}))
	}

	slog.SetDefault(logger)
	logger.Debug("debug messages are enabled.")

	return logger
}

func setupDatabase() *sql.DB {
	slog.Info("connecting to the database...")
	connStr := fmt.Sprintf("user=%s password=%s host=%s port=%s dbname=%s sslmode=disable",
		cfg.DbSettings.User,
		cfg.DbSettings.Password,
		cfg.DbSettings.Host,
		cfg.DbSettings.Port,
		cfg.DbSettings.Name,
	)
	database, err := sql.Open("postgres", connStr)
	if err != nil {
		slog.Error("failed to establish database connection.", slog.String("err", err.Error()))
		os.Exit(1)
	}
	database.SetConnMaxLifetime(cfg.DbSettings.ConnMaxLifetime)
	database.SetMaxOpenConns(cfg.DbSettings.MaxOpenConns)
	database.SetMaxIdleConns(cfg.DbSettings.MaxIdleConns)

	maxRetry := 6
	for i := 1; i <= maxRetry; i++ {
		slog.Info("ping the database.", slog.String("attempt", fmt.Sprintf("%d/%d", i, maxRetry)))
		pingErr := database.Ping()
		if pingErr != nil {
			slog.Error("not responding.", slog.String("err", pingErr.Error()))
			if i == maxRetry {
				slog.Error("failed to establish database connection.")
				os.Exit(1)
			}
			slog.Info(fmt.Sprintf("wait %d seconds", 5*i))
			time.Sleep(time.Duration(5*i) * time.Second)
		} else {
			break
		}
	}
	slog.Info("connected to the database!")

	return database
}

func closeDatabase() {
	slog.Info("closing database connection.")
	err := db.Close()
	if err != nil {
		slog.Error("failed to close database connection.", slog.String("err", err.Error()))
	}
}

// Set -1 to use all available CPUs
func parallelWorkers() int {
	customNumCPU := cfg.WorkerSettings.WorkersNum
	if customNumCPU == -1 {
		return runtime.NumCPU()
	}
	if customNumCPU <= 0 {
		slog.Error("workers number is 0 or less than -1")
		os.Exit(1)
	}

	return customNumCPU
}

func healthCheckHandler() {
	http.HandleFunc("/ping", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("pong"))
	})
	if err := http.ListenAndServe(":"+cfg.Port, nil); err != nil {
		slog.Error("http server error", slog.String("err", err.Error()))
	}
}

func getHttpTransport() *http.Transport {
	return &http.Transport{
		MaxIdleConns:        cfg.HttpClientSettings.MaxIdleConnections,
		MaxIdleConnsPerHost: cfg.HttpClientSettings.MaxIdleConnectionsPerHost,
		MaxConnsPerHost:     cfg.HttpClientSettings.MaxConnectionsPerHost,
		IdleConnTimeout:     cfg.HttpClientSettings.IdleConnectionTimeout,
		TLSHandshakeTimeout: cfg.HttpClientSettings.TlsHandshakeTimeout,
		DialContext: (&net.Dialer{
			Timeout:   cfg.HttpClientSettings.DialTimeout,
			KeepAlive: cfg.HttpClientSettings.DialKeepAlive,
		}).DialContext,
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: cfg.HttpClientSettings.TlsInsecureSkipVerify,
		},
	}
}
