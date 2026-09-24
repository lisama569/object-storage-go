# 本地内容寻址对象存储与版本管理 — 设计与一致性说明

本包 `github.com/lisama569/object-storage-go/pkg/objectstore` 仅依赖 Go 标准库，
以本地磁盘为唯一后端，提供：按内容去重的对象写入、多版本与删除/恢复语义、
空间回收与用量统计，以及在进程异常中断后可复现的崩溃恢复。

## 1. 磁盘布局

```
<Dir>/
├── lock                       # flock 独占锁文件（跨进程互斥）
├── manifest.json              # 元数据清单（原子替换）
├── blobs/<id前2字符>/<id>     # 不可变内容块（SHA-256 十六进制）
└── staging/tmp-<随机>         # 写入中的临时文件（崩溃后由恢复清理）
```

对象名只存在于 `manifest.json` 中，从不参与文件路径构造，因此对象名可以包含
`../` 等任意字符而不会造成路径穿越；而内容块路径只接受 64 位十六进制的
SHA-256 标识，非法标识在 API 层即被拒绝。

## 2. 对象标识与去重语义

- 对象的数据标识 `ContentID = lowercase(hex(sha256(全部字节)))`，长度 64。
- 标识**只取决于字节内容**：与分块大小、写入顺序、写入进程、是否并发无关。
  `HashContent` 与 `Put` 内部使用同一哈希算法；`TestChunkStableID` 用
  {1, 7, 4096, 65537} 四种分块边界验证标识恒定。
- 写入采用「先写 staging 临时文件并边写边哈希 → fsync → rename 到
  `blobs/xx/<id>`」。目标已存在时直接复用并删除临时文件，因此：
  - 相同内容重复写入，磁盘上永远只有一份内容块（物理去重）；
  - 每次 `Put` 仍产生一个独立版本号（逻辑多版本）；
  - 空内容也能正确去重（SHA-256 空串）。
- 数据完整性有两道校验：
  - 提交时哈希即标识，内容与标识必须一致（不一致返回 `ErrChecksum`，不落盘）；
  - 读取时流式重算 SHA-256，读到损坏/被篡改内容在流末尾返回 `ErrChecksum`，
    绝不静默返回坏数据。`Config.VerifyOnOpen=true` 时打开存储还会全量校验。
- 用量统计区分 `BlobBytes`（去重后的物理占用）与 `LogicalBytes`
  （所有存活版本逻辑字节之和），其差值即去重收益。

## 3. 版本、删除与回收规则

每个对象名对应一个**只追加、按版本号升序**的版本序列，版本号来自全局单调
计数器 `Counter`。版本有四种状态：`LIVE / DELETED / RECLAIMED / CORRUPT`。

- **当前版本（current）**：版本序列的**最后一个版本**。
  - 它为 `LIVE` 时 `Get(name, 0)` 返回它；
  - 它为 `DELETED/RECLAIMED/CORRUPT` 时分别返回 `ErrDeleted /
    ErrReclaimed / ErrChecksum`，**绝不静默回退到旧版本**。
- 按版本号读取 `Get(name, versionID)` 可读取任意仍可读的历史版本。
- `Delete` 是**逻辑删除**：状态置为 `DELETED` 并记录删除时间；在宽限期内、
  GC 回收数据之前可用 `Restore` 恢复为 `LIVE`。删除已删除版本是幂等的。
- `Restore` 只接受 `DELETED` 版本；`RECLAIMED/CORRUPT` 版本永久不可恢复。
- `RunGC` 的判定（输入是清单 + 磁盘快照 + 当前时间）：
  1. 被任何 `LIVE` 版本、或仍处于宽限期内的 `DELETED` 版本引用的 blob —
     **保留**（这是「不得回收仍被任何有效版本引用的数据」的实现）；
  2. `DELETED` 版本删除时间超过 `MaxUnreferencedBlobAge` — 版本转
     `RECLAIMED`，其 blob 若无其他引用则删除；
  3. 清单引用但磁盘缺失的 blob — 版本立即转 `RECLAIMED`（自洽修复，读取得到
     明确的 `ErrReclaimed` 而不是旧内容/部分内容）；
  4. 磁盘存在但清单未登记的孤儿 blob（例如崩溃在 rename 之后、清单提交之前）
     — 恢复时补登记，超过宽限期才删除；
  5. `CORRUPT` 版本的数据不再保留。
- 顺序是「先持久化 RECLAIMED 元数据，再删数据文件，再更新索引」。任一步崩溃
  后重跑 GC 都收敛到同一结果：**重复执行是无操作的幂等过程**，不会多删任何
  仍被引用的数据。

### 读取错误如何区分原因

| 情况 | 错误 |
| --- | --- |
| 对象名从未存在 / 已无版本记录 | `ErrNotFound` |
| 对象存在但指定版本号不存在 | `ErrVersionNotFound` |
| 版本存在且为 `DELETED`（可恢复） | `ErrDeleted` |
| 版本存在但数据已被回收/丢失（不可恢复） | `ErrReclaimed` |
| 数据存在但 SHA-256 校验失败 | `ErrChecksum` |
| 存储已关闭 / 目录被其他进程占用 | `ErrClosed` / `ErrBusy` |
| 回收正在进行（非阻塞模式） | `ErrLockHeld` |

均可用 `errors.Is` 判定。

## 4. 并发与崩溃恢复下的一致性

### 并发（进程内）

- 一把 `sync.RWMutex` 串行化所有元数据变更；读取获取读锁，且**读锁的释放绑定
  到返回流的 `Close`**：因此读取整个数据流期间，删除/GC 不可能移除其底层
  内容块。
- 元数据记录在解锁前会拷贝到调用方局部，避免与原地状态修改竞争（`-race`
  通过）。
- 每次元数据变更是「内存修改 + `manifest.json` 原子替换」一个临界区，配合
  单调计数器，保证并发提交下版本号不重复、不丢失；并发读取看到的当前版本
  永远是某个完整提交后的状态。
- 回收另有容量为 1 的互斥门 `gcGate` 串行化，`GCLockTimeout=0` 时第二个
  并发 GC 立即得到 `ErrLockHeld`，>0 时按超时等待。

### 跨进程

- 打开目录即在 `lock` 上获取非阻塞 `flock(LOCK_EX)`；第二个进程/句柄得到
  `ErrBusy`。进程异常退出时内核自动释放 `flock`，不会留下永久锁。

### 崩溃恢复（`Open` 时自动执行）

关键原则：**数据先于元数据落盘，元数据永远不引用未提交的数据。**
写入各崩溃点的结果因此可穷举且确定：

| 崩溃点 | 磁盘现象 | 恢复结果 |
| --- | --- | --- |
| 写 staging 期间 | `staging/tmp-*` 残文件 | 删除残文件；无任何版本指向它 |
| rename 之前 | 同上 | 同上 |
| rename 之后、清单提交之前 | 孤儿 blob | 补登记，宽限期后由 GC 回收 |
| 清单原子替换期间 | `manifest.json.tmp-*` | 删除临时文件；旧/新完整清单二者之一 |
| GC 标记之后、删数据之前 | 清单 RECLAIMED + blob 仍在 | 状态自洽；blob 因无引用在宽限期后回收 |
| 删数据之后、索引更新之前 | 清单引用缺失 blob | 版本转 RECLAIMED，索引对齐，无悬空引用 |

`manifest.json` 采用「写临时文件 → fsync → rename → fsync 目录」，所以读回的
清单要么是旧版、要么是完整新版，不会是半截 JSON。恢复结果以
`RecoveryReport` 返回（删除的残文件、发现的孤儿、悬空引用、校验数量），
`RunGC` 返回 `GCReport`（保留/删除的 blob 与字节、转回收的版本、宽限期），
两者都有 `String()`，单测日志会打印输入、判定依据与恢复结果。

> 注：`WriteFsync=true`（默认）时保证上述崩溃安全；置为 false 仅用于对
> 持久性要求低、追求写入吞吐的场景，断电时可能回退到上一个完整清单。

## 5. 可调参数（`Config`）

| 字段 | 默认 | 说明 |
| --- | --- | --- |
| `Dir` | 必填 | 数据根目录 |
| `MaxUnreferencedBlobAge` | `1s` | 无引用内容块/已删除版本数据的回收宽限期；`0` 表示立即允许回收 |
| `GCLockTimeout` | `0` | 等待回收锁的超时；`0` 为非阻塞（撞锁返回 `ErrLockHeld`） |
| `VerifyOnOpen` | `false` | 打开时是否全量 SHA-256 校验引用中的 blob |
| `WriteFsync` | `true` | 关键落盘点是否 fsync（关闭会降低崩溃安全保证） |

## 6. API 概览

```go
s, report, err := objectstore.Open(ctx, objectstore.DefaultConfig(dir))
defer s.Close()

v, _        := s.Put(ctx, "obj", reader)            // 写入（去重、新版本）
rc, info, _ := s.Get(ctx, "obj", 0)                // 读当前版本（>0 读指定版本）
_           = rc.Close()                            // 读完关闭
versions, _ := s.Versions(ctx, "obj")              // 列全部版本（含已删/已回收）
d, _        := s.Delete(ctx, "obj", 0)             // 逻辑删除当前版本
r, _        := s.Restore(ctx, "obj", d.ID)         // 宽限期内恢复
gcrep, _    := s.RunGC(ctx)                        // 空间回收（幂等、崩溃安全）
stats, _    := s.Stats()                           // 用量统计
```

## 7. 本地验证方法

无网络、无外部服务、无凭据，全部为本地临时目录：

```bash
# 常规全量测试（仓库既有方式）
go test ./...

# 竞态检测 + 重复运行（并发测试用）
go test -race -count=3 ./...

# 稳定性重复
go test -count=10 ./...

# 查看含输入/判定依据/恢复结果的日志
go test -v ./...
```

测试覆盖：

- 并发写入去重、并发读/写一致性、并发删除/恢复可见性、并发 GC 互斥
  （`concurrent_test.go`，均通过 `-race`）；
- 写入中断恢复、GC 中断恢复、悬空引用自洽、孤儿宽限、损坏检测
  （`recovery_test.go`）；
- 重复回收幂等、共享 blob 引用安全、误删边界、回收后重写、空/大对象、
  路径穿越、context 取消清理、manifest 临时文件残留
  （`edge_test.go`、`store_test.go`）。
