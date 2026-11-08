// Package archive holds the pieces of the ledger archiver (spec/TRD §7.8)
// that don't touch Postgres: the write-once object store, the Parquet
// encoding of journal lines and events, and the manifest.
package archive

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// ErrNotWORM means the bucket doesn't have Object Lock in compliance mode
// with a default retention. The archiver never drops a partition into such
// a bucket.
var ErrNotWORM = errors.New("archive bucket is not write-once (Object Lock in compliance mode with a default retention)")

// Objects is the write-once store the archiver exports to.
type Objects interface {
	// Put stores data at key under the bucket's default retention and
	// returns the object's URI and its SHA-256 (hex).
	Put(ctx context.Context, key string, data []byte) (uri, sha string, err error)
	Get(ctx context.Context, key string) ([]byte, error)
	// CheckWORM returns ErrNotWORM unless the bucket is locked in compliance mode.
	CheckWORM(ctx context.Context) error
	// URI is the s3:// URI key will have.
	URI(key string) string
}

type S3Config struct {
	BucketURI     string // s3://bucket[/prefix]
	Endpoint      string // host:port
	AccessKey     string
	SecretKey     string
	Region        string
	Secure        bool // https
	RetentionDays uint // default retention when this store creates the bucket
	CreateBucket  bool // create the bucket (Object Lock, compliance) if missing; for local stacks
}

// S3 is Objects on any S3 API with Object Lock (AWS S3, or the local gateway in compose).
type S3 struct {
	c       *minio.Client
	bucket  string
	prefix  string
	retDays uint
	create  bool
	region  string
}

func NewS3(cfg S3Config) (*S3, error) {
	u, err := url.Parse(cfg.BucketURI)
	if err != nil || u.Scheme != "s3" || u.Host == "" {
		return nil, fmt.Errorf("ARCHIVE_BUCKET_URI must look like s3://bucket[/prefix], got %q", cfg.BucketURI)
	}
	c, err := minio.New(cfg.Endpoint, &minio.Options{
		Creds: credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, ""), Secure: cfg.Secure, Region: cfg.Region})
	if err != nil {
		return nil, fmt.Errorf("archive store: %w", err)
	}
	return &S3{c: c, bucket: u.Host, prefix: strings.Trim(u.Path, "/"), retDays: cfg.RetentionDays,
		create: cfg.CreateBucket, region: cfg.Region}, nil
}

func (s *S3) key(k string) string {
	if s.prefix == "" {
		return k
	}
	return s.prefix + "/" + k
}

// URI is the s3:// URI of key.
func (s *S3) URI(key string) string { return "s3://" + s.bucket + "/" + s.key(key) }

func (s *S3) CheckWORM(ctx context.Context) error {
	exists, err := s.c.BucketExists(ctx, s.bucket)
	if err != nil {
		return fmt.Errorf("archive bucket: %w", err)
	}
	if !exists {
		if !s.create {
			return fmt.Errorf("archive bucket %s does not exist", s.bucket)
		}
		if err := s.c.MakeBucket(ctx, s.bucket, minio.MakeBucketOptions{ObjectLocking: true, Region: s.region}); err != nil {
			return fmt.Errorf("create archive bucket: %w", err)
		}
		mode, days, unit := minio.Compliance, s.retDays, minio.Days
		if err := s.c.SetObjectLockConfig(ctx, s.bucket, &mode, &days, &unit); err != nil {
			return fmt.Errorf("set archive bucket retention: %w", err)
		}
	}
	enabled, mode, validity, _, err := s.c.GetObjectLockConfig(ctx, s.bucket)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrNotWORM, err)
	}
	if enabled != "Enabled" || mode == nil || *mode != minio.Compliance || validity == nil || *validity == 0 {
		return ErrNotWORM
	}
	return nil
}

func (s *S3) Put(ctx context.Context, key string, data []byte) (string, string, error) {
	sum := sha256.Sum256(data)
	// Object Lock buckets require a content checksum on every PUT.
	if _, err := s.c.PutObject(ctx, s.bucket, s.key(key), bytes.NewReader(data), int64(len(data)),
		minio.PutObjectOptions{SendContentMd5: true, ContentType: contentType(key)}); err != nil {
		return "", "", fmt.Errorf("put %s: %w", key, err)
	}
	return s.URI(key), hex.EncodeToString(sum[:]), nil
}

func (s *S3) Get(ctx context.Context, key string) ([]byte, error) {
	o, err := s.c.GetObject(ctx, s.bucket, s.key(key), minio.GetObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("get %s: %w", key, err)
	}
	defer o.Close()
	b, err := io.ReadAll(o)
	if err != nil {
		return nil, fmt.Errorf("get %s: %w", key, err)
	}
	return b, nil
}

func contentType(key string) string {
	if strings.HasSuffix(key, ".json") {
		return "application/json"
	}
	return "application/vnd.apache.parquet"
}
