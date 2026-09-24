package objectstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"time"
)

// recoverStorage 在打开存储时执行崩溃恢复。目录锁由 Open 持有，因此此时
// 不存在并发的其他进程，可以安全地检查与修正磁盘状态。
//
// 判定规则：
//  1. staging/ 中的所有文件与 manifest.json.tmp-* 都是“未提交写入/替换”
//     的残文件，一律删除。清单从不在 blob 提交前引用它们，删除不会造成
//     悬空引用。
//  2. 清单引用了但磁盘缺失的 blob：对应版本标记为 RECLAIMED（数据丢失，
//     读取时得到 ErrReclaimed 而非静默旧内容），并从 Content 索引移除。
//  3. 磁盘存在但清单未登记的 blob：登记为孤儿（以文件 mtime 为出生时间），
//     等待 GC 宽限期后回收，绝不立即删除（宽限期内崩溃更安全）。
//  4. 配置 VerifyOnOpen 时，对每个引用中的 blob 做全量 SHA-256 校验，
//     损坏则将版本标记为 CORRUPT（读取得到 ErrChecksum）。
func (s *Store) recoverStorage(ctx context.Context) (*RecoveryReport, error) {
	rep := &RecoveryReport{}

	// 0) 清理中断的原子替换残留（staging 残文件 + manifest 临时文件）。
	// 它们永远不会被读取，但崩溃后可能堆积，恢复时统一清除。
	staging, err := s.blobs.listStaging()
	if err != nil {
		return nil, err
	}
	for _, p := range staging {
		if err := os.Remove(p); err != nil && !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
		rep.StagingFilesRemoved = append(rep.StagingFilesRemoved, p)
	}
	manifestTmps, err := s.manifest.listTempFiles()
	if err != nil {
		return nil, err
	}
	for _, p := range manifestTmps {
		if err := os.Remove(p); err != nil && !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
		rep.StagingFilesRemoved = append(rep.StagingFilesRemoved, p)
	}

	// 1) 磁盘 blob 快照。
	onDisk, err := s.blobs.listBlobs()
	if err != nil {
		return nil, err
	}

	// 2) 引用完整性核对（先决定每个 blob 的存在性与校验结果）。
	referenced := map[ContentID]bool{}
	dangling := map[ContentID]bool{}
	corrupt := map[ContentID]bool{}
	for objName, recs := range s.data.Objects {
		for _, r := range recs {
			if r.State == VersionReclaimed {
				continue
			}
			referenced[r.ContentID] = true
			if _, ok := onDisk[r.ContentID]; !ok {
				if !dangling[r.ContentID] {
					rep.DanglingRefsFound = append(rep.DanglingRefsFound, DanglingRef{
						Object: objName, VersionID: r.ID, ContentID: r.ContentID,
						Reason: "blob file missing",
					})
				}
				dangling[r.ContentID] = true
				continue
			}
			if s.cfg.VerifyOnOpen && r.State != VersionDeleted {
				ok, err := s.verifyBlob(r.ContentID)
				if err != nil {
					return nil, err
				}
				if !ok {
					rep.DanglingRefsFound = append(rep.DanglingRefsFound, DanglingRef{
						Object: objName, VersionID: r.ID, ContentID: r.ContentID,
						Reason: "sha256 mismatch",
					})
					corrupt[r.ContentID] = true
				}
			}
		}
	}

	changed := false
	if len(dangling) > 0 || len(corrupt) > 0 {
		for objName, recs := range s.data.Objects {
			for i := range recs {
				switch {
				case dangling[recs[i].ContentID]:
					recs[i].State = VersionReclaimed
					recs[i].DeletedAt = time.Now().UTC()
					s.data.Objects[objName][i] = recs[i]
					changed = true
				case corrupt[recs[i].ContentID]:
					recs[i].State = VersionCorrupt
					s.data.Objects[objName][i] = recs[i]
					changed = true
				}
			}
		}
	}
	rep.BlobsVerified = len(referenced) - len(dangling)

	// 3) Content 索引与磁盘对齐：
	//    - 索引中有、磁盘无 -> 删除索引；
	//    - 磁盘有、索引无 -> 补登记为孤儿（GC 按宽限回收）。
	for id := range s.data.Content {
		if _, ok := onDisk[id]; !ok {
			delete(s.data.Content, id)
			changed = true
		}
	}
	for id, size := range onDisk {
		if _, ok := s.data.Content[id]; !ok {
			info, err := s.blobs.statInfo(id)
			if err != nil {
				return nil, err
			}
			born := time.Now().UTC()
			if info != nil {
				born = info.ModTime().UTC()
			}
			s.data.Content[id] = BlobMeta{Size: size, CreatedAt: born}
			rep.OrphanBlobsFound = append(rep.OrphanBlobsFound, id)
			changed = true
		}
	}

	rep.changedFlag = changed
	return rep, nil
}

// verifyBlob 全量读取并校验一个 blob 的 SHA-256。
func (s *Store) verifyBlob(id ContentID) (bool, error) {
	rc, err := s.blobs.Open(id)
	if err != nil {
		return false, err
	}
	defer rc.Close()
	buf := make([]byte, hashBlockSize)
	for {
		_, err := rc.Read(buf)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return true, nil
			}
			if errors.Is(err, ErrChecksum) {
				return false, nil
			}
			return false, err
		}
	}
}

// RecoveryReport 描述一次恢复的输入、判定依据与结果。
type RecoveryReport struct {
	StagingFilesRemoved []string
	OrphanBlobsFound    []ContentID
	DanglingRefsFound   []DanglingRef
	BlobsVerified       int

	changedFlag bool
}

func (r *RecoveryReport) changed() bool { return r.changedFlag }

// DanglingRef 描述清单引用了但磁盘上缺失/损坏的 blob。
type DanglingRef struct {
	Object    string
	VersionID int64
	ContentID ContentID
	Reason    string
}

// String 以人类可读形式汇总恢复输入判定与结果。
func (r *RecoveryReport) String() string {
	return fmt.Sprintf("RecoveryReport{stagingRemoved:%d orphanBlobs:%d danglingRefs:%d blobsVerified:%d}",
		len(r.StagingFilesRemoved), len(r.OrphanBlobsFound), len(r.DanglingRefsFound), r.BlobsVerified)
}
