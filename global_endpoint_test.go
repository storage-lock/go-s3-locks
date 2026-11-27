package s3_locks

import (
	"context"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/stretchr/testify/assert"
)

var minioCmd *exec.Cmd

func TestMain(m *testing.M) {
	envName := "STORAGE_LOCK_S3_ENDPOINT"
	if os.Getenv(envName) != "" {
		os.Exit(m.Run())
	}

	// 尝试用 Docker 启动 MinIO
	port := "9000"
	consolePort := "9001"
	minioCmd = exec.Command("docker", "run", "--rm",
		"-p", port+":9000",
		"-p", consolePort+":9001",
		"-e", "MINIO_ROOT_USER=minioadmin",
		"-e", "MINIO_ROOT_PASSWORD=minioadmin",
		"--name", "s3-locks-test",
		"minio/minio:RELEASE.2024-05-28T17-19-04Z", "server", "/data", "--console-address", ":9001",
	)
	if err := minioCmd.Start(); err != nil {
		os.Exit(0)
	}

	// 等待 MinIO 就绪
	time.Sleep(time.Second * 5)
	os.Setenv("STORAGE_LOCK_S3_ENDPOINT", "localhost:"+port)
	os.Setenv("STORAGE_LOCK_S3_ACCESS_KEY", "minioadmin")
	os.Setenv("STORAGE_LOCK_S3_SECRET_KEY", "minioadmin")
	os.Setenv("STORAGE_LOCK_S3_BUCKET", "storage-lock-test")
	exitCode := m.Run()

	minioCmd.Process.Kill()
	minioCmd.Wait()
	exec.Command("docker", "rm", "-f", "s3-locks-test").Run()
	os.Exit(exitCode)
}

func TestNewS3LockByEndpoint(t *testing.T) {
	endpoint := os.Getenv("STORAGE_LOCK_S3_ENDPOINT")
	if endpoint == "" {
		t.Skip("STORAGE_LOCK_S3_ENDPOINT not set and MinIO not available")
	}

	ctx := context.Background()
	config := &EndpointConfig{
		Endpoint:  endpoint,
		AccessKey: "minioadmin",
		SecretKey: "minioadmin",
		UseSSL:    false,
		Bucket:    "storage-lock-test",
	}

	// 先确保 bucket 存在
	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4("minioadmin", "minioadmin", ""),
		Secure: false,
	})
	assert.Nil(t, err)
	exists, err := client.BucketExists(ctx, "storage-lock-test")
	if err != nil {
		t.Skipf("对象存储不可用，跳过集成测试: %v", err)
	}
	if !exists {
		err = client.MakeBucket(ctx, "storage-lock-test", minio.MakeBucketOptions{})
		assert.Nil(t, err)
	}

	// 清理旧数据
	prefix := "storage_lock_test/lock/"
	ch := client.ListObjects(ctx, "storage-lock-test", minio.ListObjectsOptions{Prefix: prefix, Recursive: true})
	for obj := range ch {
		client.RemoveObject(ctx, "storage-lock-test", obj.Key, minio.RemoveObjectOptions{})
	}

	// 运行锁测试
	factory, err := GetS3LockFactoryByEndpoint(ctx, config)
	assert.Nil(t, err)
	assert.NotNil(t, factory)

	lock, err := factory.CreateLock("test-lock")
	assert.Nil(t, err)

	// 简单测试锁的获取和释放
	err = lock.Lock(ctx, "test-lock")
	assert.Nil(t, err)

	err = lock.UnLock(ctx, "test-lock")
	assert.Nil(t, err)
}