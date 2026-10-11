package s3_locks

import (
	"context"

	"github.com/minio/minio-go/v7"
	s3_storage "github.com/storage-lock/go-s3-storage"
	"github.com/storage-lock/go-storage"
	storage_lock "github.com/storage-lock/go-storage-lock"
	storage_lock_factory "github.com/storage-lock/go-storage-lock-factory"
)

// s3ClientBeanFactory 缓存基于 S3 客户端的锁工厂
var s3ClientBeanFactory = storage_lock_factory.NewStorageLockFactoryBeanFactory[*minio.Client, *minio.Client]()

// newS3Storage 指向 s3_storage.NewS3Storage，包级变量以便单测注入构造失败，
// 覆盖 GetOrInit initFunc 的错误分支。client/endpoint 双入口共用。
var newS3Storage = func(ctx context.Context, options *s3_storage.S3StorageOptions) (storage.Storage, error) {
	return s3_storage.NewS3Storage(ctx, options)
}

// NewS3LockByClient 从已有 S3 客户端创建分布式锁
func NewS3LockByClient(ctx context.Context, client *minio.Client, bucketName string, lockId string) (*storage_lock.StorageLock, error) {
	factory, err := GetS3LockFactoryByClient(ctx, client, bucketName)
	if err != nil {
		return nil, err
	}
	return factory.CreateLock(lockId)
}

// NewS3LockByClientWithOptions 从已有 S3 客户端和锁选项创建分布式锁
func NewS3LockByClientWithOptions(ctx context.Context, client *minio.Client, bucketName string, options *storage_lock.StorageLockOptions) (*storage_lock.StorageLock, error) {
	factory, err := GetS3LockFactoryByClient(ctx, client, bucketName)
	if err != nil {
		return nil, err
	}
	return factory.CreateLockWithOptions(options)
}

// GetS3LockFactoryByClient 获取基于 S3 客户端的锁工厂（单例模式）
func GetS3LockFactoryByClient(ctx context.Context, client *minio.Client, bucketName string) (*storage_lock_factory.StorageLockFactory[*minio.Client], error) {
	return s3ClientBeanFactory.GetOrInit(ctx, client, func(ctx context.Context) (*storage_lock_factory.StorageLockFactory[*minio.Client], error) {
		cm := s3_storage.NewS3ConnectionManagerFromClient(client)
		options := s3_storage.NewS3StorageOptions().
			SetConnectionManager(cm).
			SetBucketName(bucketName).
			SetTimeProvider(&s3_storage.LocalTimeProvider{})
		storage, err := newS3Storage(ctx, options)
		if err != nil {
			return nil, err
		}
		return storage_lock_factory.NewStorageLockFactory[*minio.Client](storage, nil), nil
	})
}
