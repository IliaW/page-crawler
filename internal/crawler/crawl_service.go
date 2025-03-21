package crawler

import (
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"regexp"
	"time"

	"github.com/IliaW/page-crawler/config"
	"github.com/IliaW/page-crawler/internal/model"
	jsoniter "github.com/json-iterator/go"
	"github.com/karust/gogetcrawl/common"
	"github.com/karust/gogetcrawl/commoncrawl"
	"github.com/patrickmn/go-cache"
)

const indexListUrl = "https://index.commoncrawl.org/collinfo.json"

type Index struct {
	Id       string `json:"id"`
	Name     string `json:"name"`
	Timegate string `json:"timegate"`
	CdxAPI   string `json:"cdx-api"`
}

type CommonCrawlerService struct {
	crawler    *commoncrawl.CommonCrawl
	cfg        *config.CrawlerConfig
	localCache *cache.Cache
}

// NewCrawlService has small request limitations.
// TODO: A proxy server may be needed if we go beyond the limits
func NewCrawlService(cfg *config.CrawlerConfig) *CommonCrawlerService {
	c, err := commoncrawl.New(cfg.RequestTimeout, cfg.Retries)
	if err != nil {
		slog.Error("failed to create common crawl client", slog.String("err", err.Error()))
	}
	return &CommonCrawlerService{
		crawler:    c,
		cfg:        cfg,
		localCache: cache.New(72*time.Hour, 72*time.Hour), // CommonCrawl indexes update every month
	}
}

func (c *CommonCrawlerService) GetCrawl(crawl *model.Crawl) (*model.Crawl, error) {
	slog.Info("crawling with Common Crawl.", slog.String("url", crawl.FullURL))
	startTime := time.Now()
	if c.crawler == nil { // due to request limitations, the crawler may not be initialized when the application starts
		slog.Info("connection retry to common crawl.")
		var err error
		c.crawler, err = commoncrawl.New(c.cfg.RequestTimeout, c.cfg.Retries)
		if err != nil {
			return crawl, errors.New(fmt.Sprintf("connection to common crawl failed: %v", err.Error()))
		}
	}
	crawl.CrawlMechanism = c.crawler.Name()

	indexList, err := c.getIndexes()
	if err != nil {
		return crawl, err
	}
	requestCfg := common.RequestConfig{
		URL:     crawl.FullURL,
		Filters: []string{"statuscode:200", "mimetype:text/html"},
	}

	for i := 0; i < c.cfg.LastCrawlIndexes; i++ {
		p, _ := c.crawler.GetPagesIndex(requestCfg, indexList[i].Id)
		if len(p) == 0 {
			slog.Debug("no crawls found in Common Crawl.", slog.String("url", crawl.FullURL),
				slog.String("index", indexList[i].Id))
			continue
		}
		resp, err := c.crawler.GetFile(p[len(p)-1]) // last one is the most recent
		if err != nil {
			slog.Error("failed to get file", slog.String("err", err.Error()))
			break
		}
		body := string(resp)
		crawl.Title = extractTitle(&body)
		crawl.FullHTML = extractHtml(&body)
		crawl.StatusCode = http.StatusOK
		crawl.Status = http.StatusText(http.StatusOK)
		crawl.ETag = extractEtag(&body)
		break
	}
	if crawl.FullHTML == "" || crawl.StatusCode == 0 {
		return crawl, errors.New(fmt.Sprintf("no crawls found in Common Crawl. url: %v", crawl.FullURL))
	}
	crawl.TimeToCrawl = time.Since(startTime).Milliseconds()

	return crawl, nil
}

func (c *CommonCrawlerService) getIndexes() ([]Index, error) {
	if i, ok := c.localCache.Get("indexes"); ok {
		return i.([]Index), nil
	}

	response, err := common.Get(indexListUrl, c.crawler.MaxTimeout, c.crawler.MaxRetries)
	if err != nil {
		return nil, err
	}

	var indexes []Index
	err = jsoniter.Unmarshal(response, &indexes)
	if err != nil {
		return indexes, err
	}
	c.localCache.Set("indexes", indexes, cache.DefaultExpiration)

	return indexes, nil
}

func extractEtag(body *string) string {
	r := regexp.MustCompile(`(?i)ETag:\s*"([^"]+)"`)
	match := r.FindStringSubmatch(*body)

	if len(match) > 1 {
		return match[1]
	}
	return ""
}

func extractTitle(body *string) string {
	re := regexp.MustCompile(`<title>(.*?)</title>`)
	match := re.FindStringSubmatch(*body)

	if len(match) > 1 {
		return match[1]
	}
	return ""
}

func extractHtml(body *string) string {
	re := regexp.MustCompile(`(?si)<!doctype html>.*?</html>`)
	match := re.FindStringSubmatch(*body)

	if len(match) > 0 {
		return match[0]
	}
	return ""
}
