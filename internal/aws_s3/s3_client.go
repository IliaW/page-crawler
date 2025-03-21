package aws_s3

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	netUrl "net/url"
	"os"

	"github.com/IliaW/page-crawler/config"
	"github.com/IliaW/page-crawler/internal"
	"github.com/IliaW/page-crawler/internal/model"
	awsCfg "github.com/aws/aws-sdk-go-v2/config"
	crd "github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type BucketClient interface {
	WriteCrawl(*model.Crawl) (string, error)
}

type S3BucketClient struct {
	client *s3.Client
	cfg    *config.Config
}

func NewS3BucketClient(cfg *config.Config) *S3BucketClient {
	slog.Info("connecting to s3...")

	c, err := connect(cfg)
	if err != nil {
		slog.Error("failed to connect to s3.", slog.String("err", err.Error()))
		os.Exit(1)
	}

	return &S3BucketClient{
		client: c,
		cfg:    cfg,
	}
}

func (bc *S3BucketClient) WriteCrawl(crawl *model.Crawl) (string, error) {
	u, err := netUrl.Parse(crawl.FullURL)
	if err != nil {
		slog.Error("failed to parse url.", slog.String("url", crawl.FullURL), slog.String("err", err.Error()))
		return "", err
	}
	s3Key := fmt.Sprintf("%s/%s/%s/%s", bc.cfg.S3Settings.KeyPrefix, u.Host, internal.HashURL(crawl.FullURL),
		"crawl.json")
	body, err := json.Marshal(crawl)
	if err != nil {
		slog.Error("marshaling failed.", slog.String("err", err.Error()))
		return "", err
	}

	_, err = bc.client.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: &bc.cfg.S3Settings.BucketName,
		Key:    &s3Key,
		Body:   bytes.NewReader(body),
	})
	if err != nil {
		slog.Error("failed to save crawl to s3.", slog.String("err", err.Error()))
		return "", err
	}
	slog.Debug("crawl saved to s3.")

	return s3Key, nil
}

func connect(cfg *config.Config) (*s3.Client, error) {
	s3Config, err := awsCfg.LoadDefaultConfig(context.Background(), awsCfg.WithRegion(cfg.S3Settings.Region))
	if err != nil {
		slog.Error("failed to load s3 config.", slog.String("err", err.Error()))
		return nil, err
	}

	if cfg.Env == "local" {
		s3Config.BaseEndpoint = &cfg.S3Settings.AwsBaseEndpoint // for LocalStack
		s3Config.Credentials = crd.NewStaticCredentialsProvider("test", "test", "")
		// LocalStack does not support `virtual host addressing style` that uses s3 by default.
		// For test purposes use configuration with disabled 'virtual hosted bucket addressing'.
		// Set 'local' Env variable to use this configuration.
		slog.Warn("test configuration for S3")
		return s3.NewFromConfig(s3Config, func(o *s3.Options) {
			o.UsePathStyle = true
		}), nil
	}

	return s3.NewFromConfig(s3Config), nil
}
