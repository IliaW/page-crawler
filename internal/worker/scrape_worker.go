package worker

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/IliaW/page-crawler/config"
	"github.com/IliaW/page-crawler/internal/aws_s3"
	"github.com/IliaW/page-crawler/internal/broker"
	"github.com/IliaW/page-crawler/internal/cache"
	"github.com/IliaW/page-crawler/internal/crawler"
	"github.com/IliaW/page-crawler/internal/model"
	"github.com/IliaW/page-crawler/internal/persistence"
	"github.com/IliaW/page-crawler/internal/telemetry"
	"github.com/chromedp/cdproto/dom"
	"github.com/chromedp/cdproto/network"
	"github.com/chromedp/cdproto/page"
	"github.com/chromedp/chromedp"
	"github.com/gocolly/colly"
)

type CrawlWorker struct {
	UrlChan        <-chan []byte
	ProcessorChan  chan<- *model.ProcessorTask
	Crawl          *crawler.CommonCrawlerService
	Cfg            *config.Config
	Db             persistence.MetadataStorage
	S3             aws_s3.BucketClient
	Cache          cache.CachedClient
	Wg             *sync.WaitGroup
	CrawlMechanism model.CrawlMechanism
	KafkaDLQ       *broker.KafkaDLQClient
	Metrics        *telemetry.AppMetrics
	HttpTransport  *http.Transport
}

func (w *CrawlWorker) Run() {
	defer w.Wg.Done()
	slog.Debug("starting crawl worker.")

	for value := range w.UrlChan {
		var task model.CrawlTask
		if err := json.Unmarshal(value, &task); err != nil {
			slog.Error("failed to unmarshal message.", slog.String("err", err.Error()))
			w.KafkaDLQ.SendUrlToDLQ(string(value), err)
			w.Metrics.FailedProcessedMsgCounter(1)
			continue
		}

		crawl := &model.Crawl{
			FullURL:            task.URL,
			CrawlWorkerVersion: w.Cfg.Version,
		}
		err := error(nil)
		if task.IsAllowedToCrawl {
			crawl, err = w.crawlUrl(crawl)
			if err != nil {
				slog.Error("crawling failed.", slog.String("err", err.Error()))
				//TODO: very high load on the CommonCrawl and as a result blocking.
				//w.Metrics.WebArchiveCounter(1)
				//crawl, err = w.Crawl.GetCrawl(crawl) // try to use common crawl
				//if err != nil {
				//	slog.Error("crawling failed.", slog.String("err", err.Error()))
				//	w.KafkaDLQ.SendUrlToDLQ(task.URL, err)
				//	w.Metrics.FailedProcessedMsgCounter(1)
				//	continue
				//}
				w.KafkaDLQ.SendUrlToDLQ(task.URL, err)
				w.Metrics.FailedProcessedMsgCounter(1)
				continue
			}
			// Retries with exponential backoff for 429 status code
			for retry, delay := w.Cfg.WorkerSettings.RetryAttempts, w.Cfg.WorkerSettings.RetryDelay; crawl.StatusCode ==
				http.StatusTooManyRequests && retry > 0; retry, delay = retry-1, delay*2 {
				slog.Warn("too many requests status code. retrying...", slog.Int("attempts left", retry))
				time.Sleep(delay)
				crawl, err = w.crawlUrl(crawl)
				if err != nil {
					slog.Error("crawling failed.", slog.String("err", err.Error()))
					break
				}
			}
			if crawl.StatusCode/100 != 2 {
				slog.Error("error status code", slog.String("url", task.URL),
					slog.Int("status_code", crawl.StatusCode), slog.String("status", crawl.Status))
				w.KafkaDLQ.SendUrlToDLQ(task.URL, errors.New("error status code: "+strconv.Itoa(crawl.StatusCode)))
				w.Metrics.FailedProcessedMsgCounter(1)
				continue
			}
		} else {
			w.Metrics.CrawlingNotAllowedCounter(1)
			w.Metrics.WebArchiveCounter(1)
			crawl, err = w.Crawl.GetCrawl(crawl)
			if err != nil {
				slog.Error("CommonCrawl failed.", slog.String("err", err.Error()))
				w.KafkaDLQ.SendUrlToDLQ(task.URL, err)
				w.Metrics.FailedProcessedMsgCounter(1)
				continue
			}
		}
		w.saveCrawl(crawl, task.Force)
	}
}

func (w *CrawlWorker) saveCrawl(crawl *model.Crawl, force bool) {
	slog.Debug("Saving crawl", slog.String("Title", crawl.Title),
		slog.String("FullURL", crawl.FullURL),
		slog.Int64("TimeToCrawl", crawl.TimeToCrawl),
		slog.Int("StatusCode", crawl.StatusCode),
		slog.String("Status", crawl.Status),
		slog.String("CrawlMechanism", crawl.CrawlMechanism),
		slog.String("CrawlWorkerVersion", crawl.CrawlWorkerVersion),
		slog.String("ETag", crawl.ETag),
	)

	w.Cache.DecrementThreshold(crawl.FullURL) // Decrease threshold for the url in Cache
	s3Key, err := w.S3.WriteCrawl(crawl)      // Save the crawl to S3
	if err != nil {
		slog.Error("failed to save crawl to S3.", slog.String("url", crawl.FullURL))
		w.KafkaDLQ.SendUrlToDLQ(crawl.FullURL, err)
		w.Metrics.FailedProcessedMsgCounter(1)
		return
	}

	classifyTask := &model.ProcessorTask{
		S3Bucket: w.Cfg.S3Settings.BucketName,
		S3Key:    s3Key,
		Force:    force,
	}

	w.Cache.SaveCrawlInfo(crawl.FullURL, classifyTask) // Save info about the crawling
	if !force {
		w.Db.Save(crawl) // Save metadata to database
	}
	w.ProcessorChan <- classifyTask // Send the S3 key to kafka
	w.Metrics.SuccessfullyProcessedMsgCnt(1)
}

func (w *CrawlWorker) crawlUrl(s *model.Crawl) (*model.Crawl, error) {
	switch w.CrawlMechanism {
	case model.Curl:
		return w.crawlWithCurl(s)
	case model.HeadlessBrowser:
		return w.crawlWithBrowser(s)
	default:
		return nil, errors.New("unsupported crawl mechanism")
	}
}

func (w *CrawlWorker) crawlWithCurl(crawl *model.Crawl) (*model.Crawl, error) {
	crawl.CrawlMechanism = w.CrawlMechanism.String()

	c := colly.NewCollector()
	c.WithTransport(w.HttpTransport)
	c.SetRequestTimeout(w.Cfg.HttpClientSettings.RequestTimeout)
	c.UserAgent = w.Cfg.WorkerSettings.UserAgent

	c.OnResponse(func(resp *colly.Response) {
		crawl.FullHTML = string(resp.Body)
		crawl.ETag = resp.Headers.Get("ETag")
	})
	c.OnHTML("title", func(e *colly.HTMLElement) {
		crawl.Title = e.Text
	})

	c.OnError(func(r *colly.Response, err error) {
		crawl.StatusCode = -1
		if len(err.Error()) > 1000 {
			crawl.Status = err.Error()[:1000]
		} else {
			crawl.Status = err.Error()
		}
	})

	t := time.Now()
	err := c.Visit(crawl.FullURL)
	crawl.TimeToCrawl = time.Since(t).Milliseconds()
	if err != nil {
		return crawl, err
	}
	crawl.StatusCode = http.StatusOK
	crawl.Status = http.StatusText(200)

	return crawl, nil
}

func (w *CrawlWorker) crawlWithBrowser(crawl *model.Crawl) (*model.Crawl, error) {
	startTime := time.Now()
	crawl.CrawlMechanism = w.CrawlMechanism.String()
	responseHeaders := make(map[string]interface{}, 20)

	tCtx, cancelTCtx := context.WithTimeout(context.Background(), w.Cfg.HttpClientSettings.RequestTimeout)
	defer cancelTCtx()
	ctx, cancel := chromedp.NewContext(tCtx)
	defer cancel()

	chromedp.ListenTarget(ctx, func(event interface{}) {
		switch responseReceivedEvent := event.(type) {
		case *network.EventResponseReceived:
			response := responseReceivedEvent.Response
			if response.URL == crawl.FullURL || response.URL == crawl.FullURL+"/" {
				crawl.StatusCode = int(response.Status)
				if len(response.StatusText) > 1000 {
					crawl.Status = response.StatusText[:1000]
				} else {
					crawl.Status = response.StatusText
				}
				responseHeaders = response.Headers
			}
		case *network.EventRequestWillBeSent:
			request := responseReceivedEvent.Request
			if responseReceivedEvent.RedirectResponse != nil {
				crawl.FullURL = request.URL
				slog.Info("redirected.", slog.String("url",
					responseReceivedEvent.RedirectResponse.URL))
			}
		}
	})
	err := chromedp.Run(ctx,
		chromedp.Tasks{
			network.Enable(),
			network.SetExtraHTTPHeaders(map[string]interface{}{
				"User-Agent": w.Cfg.WorkerSettings.UserAgent,
			}),
			enableLifeCycleEvents(),
			navigateAndWaitFor(crawl.FullURL, "networkIdle"),
		},
		chromedp.Title(&crawl.Title),
		chromedp.ActionFunc(func(ctx context.Context) error {
			rootNode, err := dom.GetDocument().Do(ctx)
			if err != nil {
				return err
			}
			crawl.FullHTML, err = dom.GetOuterHTML().WithNodeID(rootNode.NodeID).Do(ctx)
			return err
		}),
	)
	if responseHeaders["ETag"] != nil {
		crawl.ETag = responseHeaders["ETag"].(string)
	}
	crawl.TimeToCrawl = time.Since(startTime).Milliseconds()

	return crawl, err
}

func enableLifeCycleEvents() chromedp.ActionFunc {
	return func(ctx context.Context) error {
		err := page.Enable().Do(ctx)
		if err != nil {
			return err
		}
		err = page.SetLifecycleEventsEnabled(true).Do(ctx)
		if err != nil {
			return err
		}
		return nil
	}
}

func navigateAndWaitFor(url string, eventName string) chromedp.ActionFunc {
	return func(ctx context.Context) error {
		_, _, _, err := page.Navigate(url).Do(ctx)
		if err != nil {
			return err
		}
		return waitFor(ctx, eventName)
	}
}

func waitFor(ctx context.Context, eventName string) error {
	ch := make(chan struct{})
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()
	chromedp.ListenTarget(cctx, func(ev interface{}) {
		switch e := ev.(type) {
		case *page.EventLifecycleEvent:
			if e.Name == eventName {
				cancel()
				close(ch)
			}
		}
	})
	select {
	case <-ch:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
