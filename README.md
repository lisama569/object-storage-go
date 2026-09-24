# object-storage-go

一个仅用 Go 标准库、以本地磁盘为唯一后端的内容寻址对象存储与版本管理库。

模块路径：`github.com/lisama569/object-storage-go`（与 `go.mod` 一致），
核心代码位于 `pkg/objectstore`，设计与一致性论证见 [`doc/DESIGN.md`](doc/DESIGN.md)。

## 特性

- **内容寻址与去重**：`ContentID = SHA-256(字节)`，只取决于内容，与分块/顺序
  无关；相同内容全局一份物理副本，每次写入仍是独立版本；空内容也去重。
- **原子写入与完整性**：staging 临时文件 + fsync + rename，提交前的残数据永不
  可寻址；读流全量校验 SHA-256，损坏返回 `ErrChecksum`，不静默返回坏数据。
- **多版本 / 删除 / 恢复**：单调版本号，当前版本即版本序列最后一个版本；
  删除为逻辑删除，宽限期内可恢复，超期由 GC 转为永久不可读的 RECLAIMED。
- **空间回收**：只删“不被任何 LIVE/宽限内 DELETED 版本引用且过宽限期”的
  blob；先改元数据再删数据，重复执行幂等，共享内容块在仍被引用时绝不删除。
- **崩溃自洽**：`Open` 自动清理 staging/manifest 残文件、核对引用完整性；
  缺失 blob 的版本标记 RECLAIMED（明确拒绝而非返回旧/部分内容），无悬空引用。
- **并发确定可见性**：进程内 RWMutex + 读流期间持锁，跨进程 flock 独占；
  版本号不重复不丢失，当前版本只能是某个完整提交或明确错误。

## 快速开始

```go
package main

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/lisama569/object-storage-go/pkg/objectstore"
)

func main() {
	ctx := context.Background()
	s, report, err := objectstore.Open(ctx, objectstore.DefaultConfig("./data"))
	if err != nil {
		panic(err)
	}
	defer s.Close()
	fmt.Println("recovery:", report)

	v, err := s.Put(ctx, "greeting", strings.NewReader("hello"))
	if err != nil {
		panic(err)
	}
	fmt.Println("wrote version", v.ID, "content", v.ContentID)

	rc, info, err := s.Get(ctx, "greeting", 0) // 0 = 当前版本
	if err != nil {
		panic(err)
	}
	b, _ := io.ReadAll(rc)
	_ = rc.Close()
	fmt.Println("read", info.ID, string(b))
}
```

## 测试（无网络 / 无外部服务 / 无凭据）

```bash
go test ./...                  # 全量
go test -race -count=3 ./...   # 并发 + 反复
go test -v ./...               # 含输入/判定/恢复日志
```

错误用 `errors.Is` 区分：`ErrNotFound`、`ErrVersionNotFound`、`ErrDeleted`、
`ErrReclaimed`、`ErrChecksum`、`ErrClosed`、`ErrBusy`、`ErrLockHeld`、
`ErrInvalid`。

## 文档

- [doc/DESIGN.md](doc/DESIGN.md)：标识与去重语义、版本/删除/回收规则、
  并发与崩溃一致性保证（含各崩溃点结果表）、可调参数与验证方法。
