package model

type CrawlMechanism int

const (
	Curl CrawlMechanism = iota
	HeadlessBrowser
)

func (sm CrawlMechanism) String() string {
	return [...]string{"curl", "headless browser"}[sm]
}

type Crawl struct {
	Title              string `json:"title"`
	FullURL            string `json:"full_url"`
	FullHTML           string `json:"full_html,omitempty"`
	TimeToCrawl        int64  `json:"time_to_crawl"` // in milliseconds
	StatusCode         int    `json:"status_code"`
	Status             string `json:"status"`
	CrawlMechanism     string `json:"crawl_mechanism"`
	CrawlWorkerVersion string `json:"crawl_worker_version"`
	ETag               string `json:"etag,omitempty"`
}

type CrawlTask struct {
	URL              string `json:"url"`
	IsAllowedToCrawl bool   `json:"allowed_to_crawl"`
	Force            bool   `json:"force"`
}

type ProcessorTask struct {
	S3Bucket string `json:"s3_bucket"`
	S3Key    string `json:"s3_key"`
	Force    bool   `json:"force"`
}
