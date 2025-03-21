package broker

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/IliaW/page-crawler/config"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/compress/lz4"
)

type KafkaDLQClient struct {
	kafkaWriter *kafka.Writer
	serviceName string
	cfg         *config.ProducerConfig
}

type DLQMessage struct {
	ServiceName  string
	URL          string
	ErrorMessage string
}

// NewKafkaDLQ - kafka client for dead-letter queue topic
func NewKafkaDLQ(serviceName string, cfg *config.ProducerConfig) *KafkaDLQClient {
	kafkaWriter := kafka.Writer{
		Addr:     kafka.TCP(cfg.Addr...),
		Topic:    cfg.DeadLetterTopicName,
		Balancer: &kafka.Hash{},
		Completion: func(messages []kafka.Message, err error) {
			if err != nil {
				slog.Error("failed to send messages to kafka DLQ.", slog.String("err", err.Error()))
			}
		},
		Compression: kafka.Compression(new(lz4.Codec).Code()),
	}
	return &KafkaDLQClient{
		kafkaWriter: &kafkaWriter,
		serviceName: serviceName,
		cfg:         cfg,
	}
}

func (dlq *KafkaDLQClient) SendUrlToDLQ(url string, err error) {
	batch := make([]kafka.Message, 0, 1)
	msg := DLQMessage{
		ServiceName:  dlq.serviceName,
		URL:          url,
		ErrorMessage: err.Error(),
	}

	body, err := json.Marshal(msg)
	if err != nil {
		slog.Error("marshaling error.", slog.String("err", err.Error()), slog.Any("message", msg))
		return
	}
	batch = append(batch, kafka.Message{
		Value: body,
	})

	err = dlq.kafkaWriter.WriteMessages(context.Background(), batch...)
	if err != nil {
		slog.Error("failed to send messages to dead-letter queue.", slog.String("err", err.Error()))
		return
	}
	slog.Debug("successfully sent messages to dead-letter queue.", slog.Int("batch length", len(batch)))
}
