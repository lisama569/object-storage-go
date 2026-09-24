# object-storage-go

一个**仅依赖本地磁盘**的内容寻址（content-addressable）、带多版本的对象存储。
纯 Go 标准库实现，无网络、无外部服务、无凭据。它面向"单目录、单写者进程 +
进程内多 goroutine"的场景，目标是让**并发写入、版本变更与空间回收在进程异常
中断后仍然一致、可复现、可解释**。

- 模块路径：`github.com/lisama569/object-storage-go`
- 包：`objectstore`（目录 `objectstore/`）
- Go 版本：见 `go.mod`

---

## 1. 快速开始

```go
import (
    "context"
    "io"
    "strings"

    "github.com/lisama569/object-storage-go/objectstore"
)

ctx := context.Background()
store, err := objectstore.Open(ctx, "/var/lib/myobj", objectstore.Options{})
if err != nil { /* ... */ }
defer store.Close()

// 写入：相同内容去重，不产生第二个数据副本
vi, err := store.Put(ctx, "photos/cat.png", strings.NewReader("...bytes..."))

// 读取当前版本（读取时自动做完整性校验）
rc, info, err := store.Get(ctx, "photos/cat.png")
defer rc.Close()
data, _ := io.ReadAll(rc)
```

---

## 2. 磁盘布局

```
<root>/
├── .lock                     # flock 进程级互斥锁
├── manifest.json             # 已提交的元数据（原子替换）
├── blobs/
│   └── <aa>/<full-sha256>    # 不可变数据文件，按摘要前两位分片
└── tmp/
    └── stage-*               # 正在写入的暂存文件（崩溃后可安全清理）
```

- **数据文件不可变**：blob 安装后永不原地修改，避免"读一半被改写"。
- **元数据是单一文档** `manifest.json`，采用"写临时文件 → fsync → rename →
  fsync 目录"的原子提交，崩溃后要么是完整旧版、要么是完整新版，绝不会出现
  半个 JSON。

---

## 3. 对象标识与去重语义

- 对象内容的标识是其字节的 SHA-256，摘要表示为 `sha256:<64 位十六进制>`。
- 标识**只取决于字节本身**：与写入顺序、`Read` 返回的分块边界、分块大小、
  写入并发时序都无关；同一进程多次运行、不同进程，结果都一致。
- 数据先以流式写入 `tmp/stage-*`，同时计算哈希；随后 rename 到
  `blobs/<aa>/<digest>`。
- **同对象去重**：若某对象的当前活跃版本与新写入字节完全相同，则**不分配新
  版本号、也不产生新的数据副本**，直接返回原版本描述符。
- **跨对象去重**：不同对象写入相同内容会各自产生逻辑版本，但物理上共享同一
  个 blob（引用计数由元数据推导）。
- 安装 blob 前会创建（必要时）分片目录并对 `blobs/` 目录 fsync，随后 fsync
  数据文件与所在分片目录，保证数据在元数据引用它之前已落盘。

### 完整性校验

- 默认开启 `VerifyOnRead`（零值 `Options{}` 即开启）：读取时对字节再做一次
  SHA-256，读到 EOF 时同时校验**长度与摘要**；任何不一致返回
  `ErrDigestMismatch`，绝不把损坏/截断数据交给调用方。
- `Verify(ctx)` 会重新哈希所有"仍被可读版本引用"的 blob，报告损坏/缺失。

---

## 4. 版本、删除、恢复规则

每次写入不同内容都会得到一个**对象内单调递增**的版本号（从 1 开始）。

每个版本有三种状态：

| 状态 | 含义 | 数据是否在盘 | 能否读 |
|------|------|--------------|--------|
| `Active` | 正常可读版本 | 是 | 是 |
| `Deleted` | 软删除（墓碑），可恢复 | 是 | `Get` 返回 `ErrDeleted` |
| `Collected` | 已物理回收，仅保留元数据 | 否 | 返回 `ErrCollected`（不可恢复） |

操作语义：

- `Put(name, r)`：新内容生成新版本并成为 current；与 current 完全相同则去重。
- `Get(name)`：读当前版本。`GetVersion(name, v)`：读指定版本。
- `Delete(name)`：对象级软删除。记录被隐藏的具体版本（`deleted_current`），
  将该版本置为 `Deleted`，`current` 指针归零（墓碑）。
- `Restore(name)`：**精确撤销** `Delete`——把 `deleted_current` 指向的版本置
  回 `Active` 并恢复为 current；不会误选其它版本。
- `DeleteVersion / RestoreVersion`：对**非当前**版本做独立的软删/恢复，不影响
  current。
- 对已删除对象再次 `Put`：以新的、更大的版本号"复活"，旧版本历史保留。
- `PurgeVersion(name, v)`：物理回收**非当前**版本。
  - current 版本永远不能被 purge（返回 `ErrCurrent`）；
  - 只有当该版本是某 blob 的**最后一个非 Collected 引用**时才删除物理文件，
    仍被任何活跃/软删版本共享的 blob 一定保留；
  - 版本记录保留为 `Collected`，之后读取明确返回 `ErrCollected`；
  - 操作幂等。

### 读取被拒时的原因是可区分的

调用方用 `errors.Is` 判定：

| 错误 | 场景 |
|------|------|
| `ErrNotFound` | 对象/版本从未存在 |
| `ErrDeleted` | 版本存在但被软删，数据仍在、可恢复 |
| `ErrCollected` | 版本已被物理回收，不可恢复 |
| `ErrCurrent` | 试图 purge 当前版本 |
| `ErrDigestMismatch` | 读到的数据与内容地址不符（损坏/截断） |
| `ErrLocked` | 另一进程已打开该目录 |
| `ErrName` | 对象名非法（空串、含 NUL、超长） |

不会出现"静默返回旧内容/部分内容"。

---

## 5. 空间回收（GC）与用量统计

- `Stats(ctx)`：不做任何删除，返回
  - `LogicalBytes`：每个**可读版本**的字节数之和（共享内容按版本计多次）；
  - `PhysicalBytes`：所有被引用 blob 的去重字节数（共享只计一次）；
  - `ReferencedBlobs / OrphanBlobs / ReclaimableBytes` 等。
- `Sweep(ctx)`：幂等回收，可重复执行、可在崩溃后重跑：
  - 默认（保守策略）只删除**真正无引用**的 blob（含崩溃写入残留、已被
    `PurgeVersion` 释放的 blob）；
  - 绝不删除仍被任意 `Active`/`Deleted` 版本引用的数据。
- 引用集合由当前 `manifest.json` 全量推导，不依赖易失的内存计数，因此重启后
  仍正确。

### 回收的崩溃安全顺序（关键）

破坏性策略下，顺序是**先提交元数据，再删数据文件**：

1. 把"仅被软删版本引用"的版本在 manifest 中标记为 `Collected` 并**原子提交**；
2. 然后才删除物理 blob。

- 在第 1 步前崩溃：什么都没发生；
- 在第 1 步与第 2 步之间崩溃：元数据已自洽（版本显示已回收），残留的只是
  无引用 blob，下一次 `Sweep` 会清掉；
- **任何时刻都不存在"元数据引用了已删 blob"的悬空状态**，也不会丢数据。
- `Sweep` 可随意重复执行，结果收敛、稳定。

---

## 6. 并发一致性

- **进程内**：`Store` 用一把互斥锁串行化所有变更；`manifest` 的原子提交保证
  多 goroutine 下版本号不丢失、不重复，读永远看到完整的旧版本或完整的新版本。
- **跨进程**：打开目录时对 `<root>/.lock` 取非阻塞 `flock(2)` 独占锁。
  第二进程 `Open` 立即得到 `ErrLocked`。`flock` 绑定打开的文件描述符，进程
  死亡（含 `SIGKILL`/断电后由 OS 回收）会自动释放，不会留下永久死锁。
- 读取路径不做"读到一半引用被清空"的操作：GC 只删无引用 blob，而当前版本必然
  有引用。

---

## 7. 崩溃恢复

- `Open` 在加锁、加载 manifest 后会自动执行一次恢复：
  1. 删除 `tmp/stage-*`（未安装的暂存数据，任何已提交元数据都不会引用它）；
  2. 删除 `.manifest-*.tmp`（未完成的元数据提交，提交版不受影响）；
  3. 遍历元数据，检查每个仍可读版本引用的 blob 是否存在，缺失则在
     `RecoveryReport.MissingBlobs` 中显式报告。
- 也可显式调用 `Recover(ctx)` 得到同样的报告。
- 不变式（invariants）：
  - 提交到 manifest 的每个引用，其 blob 必先已持久化；
  - 元数据提交是原子的；
  - 故重启后**无悬空引用、无半对象、无数据丢失**；
  - 崩溃写入的残缺数据停留在 `tmp/`，永远不会被当成有效对象读出。

---

## 8. 可调参数（`Options`）

| 字段 | 默认 | 作用 |
|------|------|------|
| `DisableReadVerification` | `false`（即默认校验） | 置 `true` 关闭读取时再哈希校验（仅在你用其它方式保证完整性时使用） |
| `SweepDeletedVersions` | `false`（保守） | 置 `true` 后 `Sweep` 会把"仅被软删版本引用"的数据也物理回收，并把这些版本转为不可逆的 `Collected` |

其余常量（如流式拷贝缓冲大小 `stageBufSize`、对象名长度上限）在源码顶部定义。

---

## 9. 本地验证

无需网络或外部依赖，直接用仓库既有的 Go 方式：

```bash
go build ./...
go vet ./...
go test ./... -v
go test -race -count=5 ./...   # 竞态检测 + 重复运行，验证稳定性
```

测试覆盖：

- 基础读写、完整性校验、损坏检测（`TestPutGetRoundTrip`、
  `TestReadVerificationDetectsCorruption`）；
- 同对象/跨对象去重、跨分块边界标识稳定（`TestDedupSameContentNoBlobCopy`、
  `TestStableIdentityAcrossChunking`）；
- 版本历史、软删/恢复的精确语义、删除后再写复活；
- 三类拒绝原因可区分（`ErrNotFound / ErrDeleted / ErrCollected`）与
  `ErrCurrent / ErrLocked / ErrName` 边界；
- **并发写入版本串行化、读写并发下无撕裂读**（`-race` 反复验证）；
- **写入中断后重开恢复**（blob 已装元数据未提交 / 仅暂存残留两种窗口）；
- **回收中断后重开继续完成**、**重复 Sweep 幂等**、保守策略不误删软删数据；
- purge 不删除被共享 blob、悬空引用被 `Recover`/`Verify` 显式报告。

每个测试都会用 `t.Logf` 打印**输入、判定依据与恢复结果**，用 `-v` 可查看。
所有数据都在 `t.TempDir()` 中，用例彼此隔离、可重复运行、结论确定。
