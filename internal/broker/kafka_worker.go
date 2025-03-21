package broker

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/IliaW/page-crawler/config"
	"github.com/IliaW/page-crawler/internal/model"
	"github.com/IliaW/page-crawler/internal/telemetry"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/compress/lz4"
)

type KafkaProducerClient struct {
	classifyChan <-chan *model.ProcessorTask
	kafkaWriter  *kafka.Writer
	metrics      *telemetry.KafkaProducerMetrics
	cfg          *config.ProducerConfig
	wg           *sync.WaitGroup
}

func NewKafkaProducer(classifyChan <-chan *model.ProcessorTask, metrics *telemetry.KafkaProducerMetrics,
	cfg *config.ProducerConfig, wg *sync.WaitGroup) *KafkaProducerClient {
	kafkaWriter := kafka.Writer{
		Addr:         kafka.TCP(cfg.Addr...),
		Topic:        cfg.WriteTopicName,
		Balancer:     &kafka.Hash{},
		MaxAttempts:  cfg.MaxAttempts,
		BatchSize:    cfg.BatchSize,
		BatchTimeout: 100 * time.Millisecond,
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
		RequiredAcks: kafka.RequiredAcks(cfg.RequiredAsks),
		Async:        cfg.Async,
		Completion: func(messages []kafka.Message, err error) {
			if err != nil {
				slog.Error("failed to send messages to kafka.", slog.String("err", err.Error()))
			}
		},
		Compression: kafka.Compression(new(lz4.Codec).Code()),
	}
	return &KafkaProducerClient{
		classifyChan: classifyChan,
		kafkaWriter:  &kafkaWriter,
		metrics:      metrics,
		cfg:          cfg,
		wg:           wg,
	}
}

func (p *KafkaProducerClient) Run() {
	slog.Info("starting kafka producer...", slog.String("topic", p.cfg.WriteTopicName))
	defer func() {
		err := p.kafkaWriter.Close()
		if err != nil {
			slog.Error("failed to close kafka writer.", slog.String("err", err.Error()))
		}
	}()
	defer p.wg.Done()

	batch := make([]kafka.Message, 0, p.cfg.BatchSize)
	batchTicker := time.NewTicker(p.cfg.BatchTimeout)
	for {
		select {
		case <-batchTicker.C:
			if len(batch) == 0 {
				continue
			}
			p.writeMessage(batch)
			batch = batch[:0]
		case task, ok := <-p.classifyChan:
			if !ok {
				if len(batch) > 0 {
					p.writeMessage(batch)
				}
				slog.Info("stopping kafka writer.")
				return
			}
			body, err := json.Marshal(task)
			if err != nil {
				slog.Error("marshaling error.", slog.String("err", err.Error()), slog.Any("task", task))
				p.metrics.FailedSendMsgCnt(1)
				continue
			}
			batch = append(batch, kafka.Message{
				Key:   []byte(task.S3Key),
				Value: body,
			})
		default:
			if len(batch) >= p.cfg.BatchSize {
				p.writeMessage(batch)
				batch = batch[:0]
				batchTicker.Reset(p.cfg.BatchTimeout)
			}
		}
	}
}

func (p *KafkaProducerClient) writeMessage(batch []kafka.Message) {
	err := p.kafkaWriter.WriteMessages(context.Background(), batch...)
	if err != nil {
		slog.Error("failed to send messages to kafka.", slog.String("err", err.Error()))
		p.metrics.FailedSendMsgCnt(int64(len(batch)))
		return
	}
	p.metrics.SuccessfullySendMsgCnt(int64(len(batch)))
	slog.Debug("successfully sent messages to kafka.", slog.Int("batch length", len(batch)))
}

type KafkaConsumerClient struct {
	urlChan chan<- []byte
	metrics *telemetry.KafkaConsumerMetrics
	cfg     *config.ConsumerConfig
	wg      *sync.WaitGroup
}

func NewKafkaConsumer(urlChan chan<- []byte, metrics *telemetry.KafkaConsumerMetrics, cfg *config.ConsumerConfig,
	wg *sync.WaitGroup) *KafkaConsumerClient {
	return &KafkaConsumerClient{
		urlChan: urlChan,
		metrics: metrics,
		cfg:     cfg,
		wg:      wg,
	}
}

func (c *KafkaConsumerClient) Run(ctx context.Context) {
	slog.Info("starting kafka consumer.", slog.String("topic", c.cfg.ReadTopicName))
	defer c.wg.Done()

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:          c.cfg.Brokers,
		Topic:            c.cfg.ReadTopicName,
		GroupID:          c.cfg.GroupID,
		MaxWait:          c.cfg.MaxWait,
		ReadBatchTimeout: c.cfg.ReadBatchTimeout,
		QueueCapacity:    c.cfg.QueueCapacity,
		MaxBytes:         c.cfg.MaxBytes,
		CommitInterval:   c.cfg.CommitInterval,
	})

	for {
		select {
		case <-ctx.Done():
			slog.Info("stopping kafka reader.")
			err := r.Close()
			if err != nil {
				slog.Error("failed to close kafka reader.", slog.String("err", err.Error()))
			}
			close(c.urlChan)
			slog.Info("close urlChan.")
			return
		default:
			m, err := r.FetchMessage(ctx)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					slog.Info("kafka reader stopped.")
					continue
				}
				slog.Error("failed to fetch message from kafka.", slog.String("err", err.Error()))
				c.metrics.FailedReadMsgCnt(1)
				continue
			}
			err = r.CommitMessages(context.Background(), m)
			if err != nil {
				slog.Error("failed to commit messages.", slog.String("err", err.Error()))
				c.metrics.FailedReadMsgCnt(1)
				continue
			}
			slog.Debug("successfully read messages from kafka.")

			c.urlChan <- m.Value
			c.metrics.SuccessfullyReadMsgCnt(1)
		}
	}
}
