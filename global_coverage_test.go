package s3_locks

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	s3_storage "github.com/storage-lock/go-s3-storage"
	"github.com/storage-lock/go-storage"
	storage_lock "github.com/storage-lock/go-storage-lock"
	"github.com/stretchr/testify/assert"
)

// testS3Config 读取 TestMain 准备好的 MinIO 端点配置。
// TestMain 在 STORAGE_LOCK_S3_ENDPOINT 未设置时会用 docker 自启 MinIO 并设置好 env。
func testS3Config(t *testing.T) (endpoint, accessKey, secretKey, bucket string) {
	t.Helper()
	endpoint = os.Getenv("STORAGE_LOCK_S3_ENDPOINT")
	if endpoint == "" {
		t.Skip("STORAGE_LOCK_S3_ENDPOINT not set and MinIO not available")
	}
	accessKey = os.Getenv("STORAGE_LOCK_S3_ACCESS_KEY")
	if accessKey == "" {
		accessKey = "minioadmin"
	}
	secretKey = os.Getenv("STORAGE_LOCK_S3_SECRET_KEY")
	if secretKey == "" {
		secretKey = "minioadmin"
	}
	bucket = os.Getenv("STORAGE_LOCK_S3_BUCKET")
	if bucket == "" {
		bucket = "storage-lock-test"
	}
	return endpoint, accessKey, secretKey, bucket
}

func newS3MinioClient(t *testing.T, endpoint, accessKey, secretKey string) *minio.Client {
	t.Helper()
	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: false,
	})
	assert.Nil(t, err)
	return client
}

// 需要真实的对象存储服务；连不上就跳过整条测试。
//
// 此处曾用 assert.Nil 断言"必须连上"，失败后测试继续执行，
// 随后对 nil factory/session 调用方法直接 panic——既掩盖了真实环境
// 差异，又让没有 MinIO 的 CI 永远变红。集成测试环境不具备时应当 skip。
func ensureS3Bucket(t *testing.T, ctx context.Context, client *minio.Client, bucket string) {
	t.Helper()
	exists, err := client.BucketExists(ctx, bucket)
	if err != nil {
		t.Skipf("对象存储不可用，跳过集成测试: %v", err)
	}
	if !exists {
		if err := client.MakeBucket(ctx, bucket, minio.MakeBucketOptions{}); err != nil {
			t.Skipf("无法创建测试 bucket，跳过集成测试: %v", err)
		}
	}
}

func TestNewS3LockByClient(t *testing.T) {
	endpoint, accessKey, secretKey, bucket := testS3Config(t)
	ctx := context.Background()
	client := newS3MinioClient(t, endpoint, accessKey, secretKey)
	ensureS3Bucket(t, ctx, client, bucket)

	factory, err := GetS3LockFactoryByClient(ctx, client, bucket)
	assert.Nil(t, err)
	assert.NotNil(t, factory)

	// 缓存命中
	factory2, err := GetS3LockFactoryByClient(ctx, client, bucket)
	assert.Nil(t, err)
	assert.Equal(t, factory, factory2)

	lock, err := NewS3LockByClient(ctx, client, bucket, "test-s3-client-lock")
	assert.Nil(t, err)
	assert.NotNil(t, lock)
	assert.Nil(t, lock.Lock(ctx, "owner-1"))
	assert.Nil(t, lock.UnLock(ctx, "owner-1"))

	opts := storage_lock.NewStorageLockOptionsWithLockId("test-s3-client-lock-opt")
	lock2, err := NewS3LockByClientWithOptions(ctx, client, bucket, opts)
	assert.Nil(t, err)
	assert.NotNil(t, lock2)
}

func TestNewS3LockByEndpointWrappers(t *testing.T) {
	endpoint, accessKey, secretKey, bucket := testS3Config(t)
	ctx := context.Background()

	// 用不同写法区分缓存 key，并带上 Region，覆盖 Region 分支
	regionEndpoint := "127.0.0.1:9000"
	if endpoint == "127.0.0.1:9000" || endpoint == "localhost:9000" {
		regionEndpoint = endpoint
		if regionEndpoint == "localhost:9000" {
			regionEndpoint = "127.0.0.1:9000"
		}
	} else {
		regionEndpoint = endpoint
	}
	config := &EndpointConfig{
		Endpoint:  regionEndpoint,
		AccessKey: accessKey,
		SecretKey: secretKey,
		UseSSL:    false,
		Region:    "us-east-1",
		Bucket:    bucket,
	}

	factory, err := GetS3LockFactoryByEndpoint(ctx, config)
	if err != nil {
		t.Skipf("对象存储不可用，跳过集成测试: %v", err)
	}
	assert.NotNil(t, factory)

	// 缓存命中
	factory2, err := GetS3LockFactoryByEndpoint(ctx, config)
	assert.Nil(t, err)
	assert.Equal(t, factory, factory2)

	lock, err := NewS3LockByEndpoint(ctx, config, "test-s3-endpoint-lock")
	assert.Nil(t, err)
	assert.NotNil(t, lock)

	opts := storage_lock.NewStorageLockOptionsWithLockId("test-s3-endpoint-lock-opt")
	lock2, err := NewS3LockByEndpointWithOptions(ctx, config, opts)
	assert.Nil(t, err)
	assert.NotNil(t, lock2)
}

func TestS3Lock_StorageError(t *testing.T) {
	old := newS3Storage
	newS3Storage = func(ctx context.Context, options *s3_storage.S3StorageOptions) (storage.Storage, error) {
		return nil, errors.New("boom")
	}
	defer func() { newS3Storage = old }()

	ctx := context.Background()

	// 独立 client 避免命中成功用例的缓存（bean factory 按指针缓存）
	errClient, err := minio.New("127.0.0.1:9999", &minio.Options{
		Creds:  credentials.NewStaticV4("boom", "boom", ""),
		Secure: false,
	})
	assert.Nil(t, err)

	_, err = GetS3LockFactoryByClient(ctx, errClient, "boom-bucket")
	assert.NotNil(t, err)
	_, err = NewS3LockByClient(ctx, errClient, "boom-bucket", "x")
	assert.NotNil(t, err)
	_, err = NewS3LockByClientWithOptions(ctx, errClient, "boom-bucket", storage_lock.NewStorageLockOptionsWithLockId("x"))
	assert.NotNil(t, err)

	// 独立 endpoint key，避免命中成功用例的缓存
	errConfig := &EndpointConfig{
		Endpoint:  "127.0.0.1:9999",
		AccessKey: "boom",
		SecretKey: "boom",
		Bucket:    "boom-bucket",
	}
	_, err = GetS3LockFactoryByEndpoint(ctx, errConfig)
	assert.NotNil(t, err)
	_, err = NewS3LockByEndpoint(ctx, errConfig, "x")
	assert.NotNil(t, err)
	_, err = NewS3LockByEndpointWithOptions(ctx, errConfig, storage_lock.NewStorageLockOptionsWithLockId("x"))
	assert.NotNil(t, err)
}
