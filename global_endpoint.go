package s3_locks

import (
	"context"

	"github.com/minio/minio-go/v7"
	s3_storage "github.com/storage-lock/go-s3-storage"
	storage_lock "github.com/storage-lock/go-storage-lock"
	storage_lock_factory "github.com/storage-lock/go-storage-lock-factory"
)

// s3EndpointBeanFactory 缓存基于端点的锁工厂
var s3EndpointBeanFactory = storage_lock_factory.NewStorageLockFactoryBeanFactory[string, *minio.Client]()

// EndpointConfig S3 端点配置
type EndpointConfig struct {
	Endpoint  string
	AccessKey string
	SecretKey string
	UseSSL    bool
	Region    string
	Bucket    string
}

// NewS3LockByEndpoint 从端点参数创建分布式锁
func NewS3LockByEndpoint(ctx context.Context, config *EndpointConfig, lockId string) (*storage_lock.StorageLock, error) {
	factory, err := GetS3LockFactoryByEndpoint(ctx, config)
	if err != nil {
		return nil, err
	}
	return factory.CreateLock(lockId)
}

// NewS3LockByEndpointWithOptions 从端点参数和锁选项创建分布式锁
func NewS3LockByEndpointWithOptions(ctx context.Context, config *EndpointConfig, options *storage_lock.StorageLockOptions) (*storage_lock.StorageLock, error) {
	factory, err := GetS3LockFactoryByEndpoint(ctx, config)
	if err != nil {
		return nil, err
	}
	return factory.CreateLockWithOptions(options)
}

// GetS3LockFactoryByEndpoint 获取基于端点的锁工厂（单例模式，endpoint 作缓存 key）
func GetS3LockFactoryByEndpoint(ctx context.Context, config *EndpointConfig) (*storage_lock_factory.StorageLockFactory[*minio.Client], error) {
	// 用 endpoint+bucket 作为缓存 key
	cacheKey := config.Endpoint + "/" + config.Bucket

	return s3EndpointBeanFactory.GetOrInit(ctx, cacheKey, func(ctx context.Context) (*storage_lock_factory.StorageLockFactory[*minio.Client], error) {
		cm := s3_storage.NewS3ConnectionManagerFromEndpoint(config.Endpoint, config.AccessKey, config.SecretKey, config.UseSSL)
		if config.Region != "" {
			cm.Region = config.Region
		}
		options := s3_storage.NewS3StorageOptions().
			SetConnectionManager(cm).
			SetBucketName(config.Bucket).
			SetTimeProvider(&s3_storage.LocalTimeProvider{})
		storage, err := newS3Storage(ctx, options)
		if err != nil {
			return nil, err
		}
		return storage_lock_factory.NewStorageLockFactory[*minio.Client](storage, nil), nil
	})
}