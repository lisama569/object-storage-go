package objectstore

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
)

// ContentID 是基于内容的 SHA-256 标识：64 字符小写十六进制字符串。
// 标识只取决于字节序列本身，因此相同内容无论分块大小、写入顺序、
// 写入进程是否相同，得到的 ContentID 都一致。
type ContentID string

// String 便于日志输出。
func (id ContentID) String() string { return string(id) }

// hashBlockSize 是流式哈希时的拷贝块大小；它只影响性能，不影响结果。
const hashBlockSize = 64 * 1024

// HashContent 计算 r 中全部数据的内容标识与字节数。
// 调用方负责关闭 r（若需要）。
func HashContent(r io.Reader) (ContentID, int64, error) {
	h := sha256.New()
	n, err := io.CopyBuffer(h, r, make([]byte, hashBlockSize))
	if err != nil {
		return "", 0, err
	}
	return ContentID(hex.EncodeToString(h.Sum(nil))), n, nil
}
