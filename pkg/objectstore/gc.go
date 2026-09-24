package objectstore

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// GCReport 描述一次空间回收的输入、判定依据与结果，便于测试与审计。
type GCReport struct {
	StartedAt  time.Time
	FinishedAt time.Time
	// 判定输入
	GracePeriod time.Duration
	// 判定结果
	LiveBlobs         int           // 仍被 LIVE/DELETED 版本引用的 blob
	LiveBytes         int64         // 仍被引用数据的字节
	DeletableBlobs    []ContentID   // 本次删除的 blob
	DeletableBytes    int64         // 本次释放的字节
	VersionsReclaimed []DanglingRef // 因数据缺失/损坏被标记 RECLAIMED 的版本
	DeletedVersions   int64         // 本次被清理数据的 DELETED 版本数
}

// RunGC 执行一次空间回收。语义：
//
//   - 只有不被任何 LIVE/DELETED 版本引用、且登记时间超过宽限期
//     MaxUnreferencedBlobAge 的 blob 才会被删除；
//   - DELETED 版本：删除时间超过宽限期后，其数据随之删除，版本转为
//     RECLAIMED（永久不可读、不可恢复）；
//   - 清单引用了但磁盘缺失的 blob：版本立即标记 RECLAIMED（自洽修复）；
//   - 全过程“先标记元数据、再删数据、最后再核对”，任一步骤崩溃后重跑
//     都收敛到同一结果；重复执行不会多删任何仍被引用的数据。
func (s *Store) RunGC(ctx context.Context) (*GCReport, error) {
	if err := s.acquireGC(); err != nil {
		return nil, err
	}
	defer s.releaseGC()

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, ErrClosed
	}

	rep := &GCReport{StartedAt: time.Now().UTC(), GracePeriod: s.cfg.MaxUnreferencedBlobAge}
	now := time.Now().UTC()
	changed := false

	// 注意：此处**不**清理 staging/——进程内可能有 Put 正在写临时文件。
	// staging 残文件只在 Open 的崩溃恢复阶段清理（此时目录锁保证没有其他写入者）。

	// 1) 计算引用集合与各版本的新状态。
	referenced := map[ContentID]bool{}
	var toReclaim []DanglingRef // 数据缺失的引用
	reclaimVersions := map[int64]bool{}
	for name, recs := range s.data.Objects {
		for i := range recs {
			r := &recs[i]
			switch r.State {
			case VersionLive:
				referenced[r.ContentID] = true
			case VersionCorrupt:
				// 数据损坏：永久不可读，按回收处理，不再占用数据。
				referenced[r.ContentID] = false
				toReclaim = append(toReclaim, DanglingRef{
					Object: name, VersionID: r.ID, ContentID: r.ContentID,
					Reason: "checksum mismatch",
				})
				reclaimVersions[r.ID] = true
			case VersionDeleted:
				// 宽限期内仍保留数据（可 Restore）；超期则回收。
				if s.cfg.MaxUnreferencedBlobAge > 0 &&
					now.Sub(r.DeletedAt) < s.cfg.MaxUnreferencedBlobAge {
					referenced[r.ContentID] = true
				} else {
					rep.DeletedVersions++
					toReclaim = append(toReclaim, DanglingRef{
						Object: name, VersionID: r.ID, ContentID: r.ContentID,
						Reason: "deleted beyond grace period",
					})
					reclaimVersions[r.ID] = true
				}
			case VersionReclaimed:
				// 不再引用。
			}
		}
	}

	// 2) 对仍应保留的引用做存在性核对：磁盘缺失则转为 RECLAIMED。
	for name, recs := range s.data.Objects {
		for i := range recs {
			r := &recs[i]
			if !referenced[r.ContentID] {
				continue
			}
			ok, err := s.blobs.Exists(r.ContentID)
			if err != nil {
				return nil, err
			}
			if !ok {
				referenced[r.ContentID] = false
				toReclaim = append(toReclaim, DanglingRef{
					Object: name, VersionID: r.ID, ContentID: r.ContentID,
					Reason: "blob file missing",
				})
				reclaimVersions[r.ID] = true
			}
		}
	}

	// 3) 应用版本状态变更（元数据先落盘，保证不会再读到已删数据）。
	if len(reclaimVersions) > 0 {
		for name, recs := range s.data.Objects {
			for i := range recs {
				if reclaimVersions[recs[i].ID] {
					recs[i].State = VersionReclaimed
					if recs[i].DeletedAt.IsZero() {
						recs[i].DeletedAt = now
					}
					s.data.Objects[name][i] = recs[i]
				}
			}
		}
		rep.VersionsReclaimed = toReclaim
		changed = true
	}

	// 4) 决定可删除 blob：不被引用且超过孤儿宽限期。
	onDisk, err := s.blobs.listBlobs()
	if err != nil {
		return nil, err
	}
	for id, size := range onDisk {
		if referenced[id] {
			rep.LiveBlobs++
			rep.LiveBytes += size
			continue
		}
		meta, known := s.data.Content[id]
		born := now
		if known {
			born = meta.CreatedAt
		}
		if s.cfg.MaxUnreferencedBlobAge > 0 && now.Sub(born) < s.cfg.MaxUnreferencedBlobAge {
			continue // 宽限期内，保留
		}
		rep.DeletableBlobs = append(rep.DeletableBlobs, id)
		rep.DeletableBytes += size
	}

	// 5) 先持久化“哪些版本已回收”的元数据，再删除数据文件。
	if changed {
		if err := s.manifest.save(s.data); err != nil {
			return nil, err
		}
	}

	// 6) 删除数据文件并更新 Content 索引；单文件失败不影响其余文件，
	//    残留项会在下次 GC 时被同样规则处理（幂等）。
	var removeErr error
	for _, id := range rep.DeletableBlobs {
		if err := s.blobs.Remove(id); err != nil {
			removeErr = errors.Join(removeErr, err)
			continue
		}
		delete(s.data.Content, id)
	}
	if removeErr != nil {
		return rep, removeErr
	}
	if len(rep.DeletableBlobs) > 0 {
		if err := s.manifest.save(s.data); err != nil {
			return nil, err
		}
	}

	rep.FinishedAt = time.Now().UTC()
	return rep, nil
}

// acquireGC 进程内回收互斥（带配置超时）。
func (s *Store) acquireGC() error {
	if s.cfg.GCLockTimeout <= 0 {
		select {
		case s.gcGate <- struct{}{}:
			return nil
		default:
			return ErrLockHeld
		}
	}
	timer := time.NewTimer(s.cfg.GCLockTimeout)
	defer timer.Stop()
	select {
	case s.gcGate <- struct{}{}:
		return nil
	case <-timer.C:
		return ErrLockHeld
	}
}

func (s *Store) releaseGC() { <-s.gcGate }

// String 以人类可读形式汇总回收判定与结果。
func (r *GCReport) String() string {
	return fmt.Sprintf("GCReport{grace:%s liveBlobs:%d liveBytes:%d deletedBlobs:%d deletedBytes:%d versionsReclaimed:%d deletedVersions:%d}",
		r.GracePeriod, r.LiveBlobs, r.LiveBytes, len(r.DeletableBlobs), r.DeletableBytes,
		len(r.VersionsReclaimed), r.DeletedVersions)
}
