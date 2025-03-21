package persistence

import (
	"database/sql"
	"log/slog"
	"time"

	"github.com/IliaW/page-crawler/internal"
	"github.com/IliaW/page-crawler/internal/model"
)

type MetadataStorage interface {
	Save(*model.Crawl)
}

type MetadataRepository struct {
	db *sql.DB
}

func NewMetadataRepository(db *sql.DB) *MetadataRepository {
	return &MetadataRepository{db: db}
}

func (mr *MetadataRepository) Save(crawl *model.Crawl) {
	_, err := mr.db.Exec(`INSERT INTO web_crawler.crawl_metadata 
    (url_hash, full_url, time_to_crawl, timestamp, status, status_code, crawl_mechanism, crawl_worker_version, e_tag)
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) 
	ON CONFLICT (url_hash) DO UPDATE 
	SET full_url = EXCLUDED.full_url,
	    time_to_crawl = EXCLUDED.time_to_crawl,
	    timestamp = EXCLUDED.timestamp,
		status = EXCLUDED.status,
		status_code = EXCLUDED.status_code,
		crawl_mechanism = EXCLUDED.crawl_mechanism,
		crawl_worker_version = EXCLUDED.crawl_worker_version,
		e_tag = EXCLUDED.e_tag;`,
		internal.HashURL(crawl.FullURL),
		crawl.FullURL,
		crawl.TimeToCrawl,
		time.Now().UTC(),
		crawl.Status,
		crawl.StatusCode,
		crawl.CrawlMechanism,
		crawl.CrawlWorkerVersion,
		crawl.ETag)
	if err != nil {
		slog.Error("failed to save crawl metadata to database.", slog.String("err", err.Error()))
		return
	}
	slog.Debug("crawl metadata saved to db.")
}
