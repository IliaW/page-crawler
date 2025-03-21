package telemetry

import (
	"context"
	"go.opentelemetry.io/otel"
	"log/slog"
	"os"

	"go.opentelemetry.io/contrib/detectors/aws/ecs"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"

	"github.com/IliaW/page-crawler/config"
	"github.com/google/uuid"
)

var meter metric.Meter

type MetricsProvider struct {
	KafkaConsumerMetrics *KafkaConsumerMetrics
	KafkaProducerMetrics *KafkaProducerMetrics
	AppMetrics           *AppMetrics
	Close                func()
}

type KafkaConsumerMetrics struct {
	SuccessfullyReadMsgCnt func(count int64)
	FailedReadMsgCnt       func(count int64)
}

type KafkaProducerMetrics struct {
	SuccessfullySendMsgCnt func(count int64)
	FailedSendMsgCnt       func(count int64)
}

type AppMetrics struct {
	SuccessfullyProcessedMsgCnt func(count int64)
	FailedProcessedMsgCounter   func(count int64)
	WebArchiveCounter           func(count int64)
	CrawlingNotAllowedCounter   func(count int64)
}

func SetupMetrics(ctx context.Context, cfg *config.Config) *MetricsProvider {
	metricsProvider := new(MetricsProvider)
	var meterProvider *sdkmetric.MeterProvider

	if cfg.TelemetrySettings.Enabled {
		r, err := newResource(cfg)
		if err != nil {
			slog.Error("failed to get resource.", slog.String("err", err.Error()))
			os.Exit(1)
		}
		exporter, err := newMetricExporter(ctx, cfg.TelemetrySettings)
		if err != nil {
			slog.Error("failed to get metric exporter.", slog.String("err", err.Error()))
			os.Exit(1)
		}
		meterProvider = newMeterProvider(exporter, *r)
		otel.SetMeterProvider(meterProvider)
	}

	meter = otel.Meter(cfg.ServiceName)
	metricsProvider.Close = func() {
		if meterProvider != nil {
			err := meterProvider.Shutdown(ctx)
			if err != nil {
				slog.Error("failed to shutdown metrics provider.", slog.String("err", err.Error()))
			}
		}
	}

	// Set up kafka consumer metrics
	kafkaConsumerSuccessCounter, err := meter.Int64Counter("crawl-worker.kafka.read.success",
		metric.WithDescription("The number of messages that the kafka consumer successfully processed"),
		metric.WithUnit("{messages}"))
	kafkaConsumerFailCounter, err := meter.Int64Counter("crawl-worker.kafka.read.fail",
		metric.WithDescription("The number of messages that the kafka consumer could not process"),
		metric.WithUnit("{messages}"))
	if err != nil {
		slog.Error("failed to create telemetry counters for kafka consumer.", slog.String("err", err.Error()))
		os.Exit(1)
	}
	metricsProvider.KafkaConsumerMetrics = &KafkaConsumerMetrics{
		SuccessfullyReadMsgCnt: func(count int64) {
			if cfg.TelemetrySettings.Enabled {
				kafkaConsumerSuccessCounter.Add(ctx, count)
			}
		},
		FailedReadMsgCnt: func(count int64) {
			if cfg.TelemetrySettings.Enabled {
				kafkaConsumerFailCounter.Add(ctx, count)
			}
		},
	}

	// Set up kafka producer metrics
	kafkaProducerSuccessCounter, err := meter.Int64Counter("crawl-worker.kafka.send.success",
		metric.WithDescription("The number of messages that the kafka producer successfully processed"),
		metric.WithUnit("{messages}"))
	kafkaProducerFailCounter, err := meter.Int64Counter("crawl-worker.kafka.send.fail",
		metric.WithDescription("The number of messages that the kafka producer could not process"),
		metric.WithUnit("{messages}"))
	if err != nil {
		slog.Error("failed to create telemetry counters for kafka producer.", slog.String("err", err.Error()))
		os.Exit(1)
	}
	metricsProvider.KafkaProducerMetrics = &KafkaProducerMetrics{
		SuccessfullySendMsgCnt: func(count int64) {
			if cfg.TelemetrySettings.Enabled {
				kafkaProducerSuccessCounter.Add(ctx, count)
			}
		},
		FailedSendMsgCnt: func(count int64) {
			if cfg.TelemetrySettings.Enabled {
				kafkaProducerFailCounter.Add(ctx, count)
			}
		},
	}

	// Set up worker metrics
	appSuccessCounter, err := meter.Int64Counter("crawl-worker.messages.success",
		metric.WithDescription("The number of messages that the worker successfully processed"),
		metric.WithUnit("{messages}"))
	appFailCounter, err := meter.Int64Counter("crawl-worker.messages.fail",
		metric.WithDescription("The number of messages that the worker could not be processed. Send to DLQ."),
		metric.WithUnit("{messages}"))
	appArchiveCounter, err := meter.Int64Counter("crawl-worker.messages.archive",
		metric.WithDescription("The number of calls to the CommonCrawl API."),
		metric.WithUnit("{messages}"))
	appCrawlingNotAllowedCounter, err := meter.Int64Counter("crawl-worker.messages.crawl-not-allowed",
		metric.WithDescription("The number of messages that the worker could not be processed due to crawling"+
			" not allowed. Use Web-Archive"),
		metric.WithUnit("{messages}"))
	if err != nil {
		slog.Error("failed to create telemetry counters fo worker.", slog.String("err", err.Error()))
		os.Exit(1)
	}
	metricsProvider.AppMetrics = &AppMetrics{
		SuccessfullyProcessedMsgCnt: func(count int64) {
			if cfg.TelemetrySettings.Enabled {
				appSuccessCounter.Add(ctx, count)
			}
		},
		FailedProcessedMsgCounter: func(count int64) {
			if cfg.TelemetrySettings.Enabled {
				appFailCounter.Add(ctx, count)
			}
		},
		WebArchiveCounter: func(count int64) {
			if cfg.TelemetrySettings.Enabled {
				appArchiveCounter.Add(ctx, count)
			}
		},
		CrawlingNotAllowedCounter: func(count int64) {
			if cfg.TelemetrySettings.Enabled {
				appCrawlingNotAllowedCounter.Add(ctx, count)
			}
		},
	}

	// initialize metrics in DataDog for setup UI
	if cfg.TelemetrySettings.Enabled {
		metricsProvider.KafkaProducerMetrics.SuccessfullySendMsgCnt(1)
		metricsProvider.KafkaProducerMetrics.FailedSendMsgCnt(1)
		metricsProvider.KafkaConsumerMetrics.SuccessfullyReadMsgCnt(1)
		metricsProvider.KafkaConsumerMetrics.FailedReadMsgCnt(1)
		metricsProvider.AppMetrics.SuccessfullyProcessedMsgCnt(1)
		metricsProvider.AppMetrics.FailedProcessedMsgCounter(1)
		metricsProvider.AppMetrics.WebArchiveCounter(1)
		metricsProvider.AppMetrics.CrawlingNotAllowedCounter(1)
	}

	return metricsProvider
}

func newResource(cfg *config.Config) (*resource.Resource, error) {
	ecsResourceDetector := ecs.NewResourceDetector()
	ecsResource, err := ecsResourceDetector.Detect(context.Background())
	if err != nil {
		slog.Error("ecs detection failed", slog.String("err", err.Error()))
	}
	mergedResource, err := resource.Merge(ecsResource, resource.Default())
	if err != nil {
		slog.Error("failed to merge resources", slog.String("err", err.Error()))
	}
	keyValue, found := ecsResource.Set().Value("container.id")
	var serviceId string
	if found {
		serviceId = keyValue.AsString()
	} else {
		serviceId = uuid.New().String()
	}
	return resource.Merge(mergedResource,
		resource.NewWithAttributes(semconv.SchemaURL,
			semconv.ServiceName(cfg.ServiceName),
			semconv.DeploymentEnvironment(cfg.Env),
			semconv.ServiceInstanceID(serviceId),
		))
}

func newMetricExporter(ctx context.Context, cfg *config.TelemetryConfig) (sdkmetric.Exporter, error) {
	return otlpmetrichttp.New(ctx,
		otlpmetrichttp.WithEndpoint(cfg.CollectorUrl),
		otlpmetrichttp.WithInsecure())
}

func newMeterProvider(meterExporter sdkmetric.Exporter, resource resource.Resource) *sdkmetric.MeterProvider {
	meterProvider := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(meterExporter)),
		sdkmetric.WithResource(&resource),
	)
	return meterProvider
}
