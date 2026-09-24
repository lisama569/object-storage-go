package objectstore

// Stats 是用量统计快照。
type Stats struct {
	Objects           int   // 对象名数量（含无存活版本的名字）
	LiveVersions      int   // 存活版本数
	DeletedVersions   int   // 已删除但数据尚未回收的版本数
	ReclaimedVersions int   // 数据已被回收的版本数
	CorruptVersions   int   // 数据损坏的版本数
	Blobs             int   // 去重后物理内容块数量（磁盘）
	BlobBytes         int64 // 物理数据占用字节（去重后）
	LogicalBytes      int64 // 所有存活版本引用的逻辑字节总和（不去重）
}

// Stats 返回当前用量统计。BlobBytes 反映实际磁盘数据占用，
// LogicalBytes 反映“如果不去重”存活版本需要的字节，二者之差即去重收益。
func (s *Store) Stats() (Stats, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return Stats{}, ErrClosed
	}

	var st Stats
	st.Objects = len(s.data.Objects)
	for _, recs := range s.data.Objects {
		for _, r := range recs {
			switch r.State {
			case VersionLive:
				st.LiveVersions++
				st.LogicalBytes += r.Size
			case VersionDeleted:
				st.DeletedVersions++
			case VersionReclaimed:
				st.ReclaimedVersions++
			case VersionCorrupt:
				st.CorruptVersions++
			}
		}
	}
	st.Blobs = len(s.data.Content)
	for _, m := range s.data.Content {
		st.BlobBytes += m.Size
	}
	return st, nil
}
