const std = @import("std");

pub const Cursor = struct {
    shard_id: u16,
    wal_segment_id: u32 = 0,
    wal_offset: u64 = 0,
};

pub const BatchOp = union(enum) {
    put: struct {
        key: []const u8,
        value: []const u8,
    },
    del: struct {
        key: []const u8,
    },
};

pub const CommitToken = struct {
    shard_id: u16,
    wal_segment_id: u32,
    wal_offset: u64,
    seqno: u64,
};

pub const GlobalCommitToken = struct {
    tokens: []CommitToken,
};

pub const StreamValue = union(enum) {
    @"inline": []u8,
    blob: []u8,
    tombstone,
};

pub const GlobalMergeShardState = struct {
    cursor: ShardCursor = .{
        .wal_segment_id = 0,
        .wal_offset = 0,
    },
    has_buffered_item: bool = false,
    buffered_item: ?StreamItem = null,
};

pub const SnapshotReadFromResult = struct {
    items: []StreamItem,
    next_cursor: GlobalMergeCursor,
};

pub const GlobalCursor = struct {
    shard_id: u16 = 0,
    wal_segment_id: u32 = 0,
    wal_offset: u64 = 0,
};

pub const StreamItem = struct {
    seqno: u64,
    key: []u8,
    value: StreamValue,
    durability: Durability,
    record_type: RecordType,
    cursor: Cursor,
};

pub const ReadFromResult = struct {
    items: []StreamItem,
    next_cursor: Cursor,
    arena: ?std.heap.ArenaAllocator = null,
};

pub const GlobalReadFromResult = struct {
    items: []StreamItem,
    next_cursor: GlobalCursor,
    arena: ?std.heap.ArenaAllocator = null,
};

pub const ShardCursor = struct {
    wal_segment_id: u32 = 0,
    wal_offset: u64 = 0,
};

pub const SnapshotEntry = struct {
    shard_id: u16,
    key: []u8,
    value: []u8,
};

pub const SnapshotShardMeta = struct {
    shard_id: u16,
    next_seqno: u64,
    visible: ShardCursor,
    durable: ShardCursor,
};

pub const StateSnapshot = struct {
    version: u32 = 1,
    shard_count: u16,
    kind: CheckpointKind,
    created_at_unix_ns: u64,
    high_seqno: u64,

    checkpoint: EngineCheckpoint,
    shard_meta: []SnapshotShardMeta,
    entries: []SnapshotEntry,
};

pub const SnapshotInspectInfo = struct {
    version: u32,
    shard_count: u16,
    kind: CheckpointKind,
    created_at_unix_ns: u64,
    high_seqno: u64,
    entry_count: u64,
};

pub const FrontierWindow = struct {
    visible: ShardCursor = .{},
    durable: ShardCursor = .{},
};

pub const CommitWindow = struct {
    shard_id: u16,
    start: ShardCursor,
    end: ShardCursor,
    visible: ShardCursor,
    durable: ?ShardCursor = null,
    record_count: usize,
    first_seqno: u64,
    last_seqno: u64,
};

pub const ReadConsistency = enum {
    visible,
    durable,
};

pub const GlobalMergeCursor = struct {
    shard_cursors: []ShardCursor,
};

pub const GlobalMergeReadFromResult = struct {
    items: []StreamItem,
    next_cursor: GlobalMergeCursor,
    arena: ?std.heap.ArenaAllocator = null,
};

pub const CheckpointKind = enum(u8) {
    visible,
    durable,
};

pub const EngineCheckpoint = struct {
    version: u32 = 1,
    shard_count: u16,
    reserved0: u16 = 0,
    kind: CheckpointKind,
    reserved1: [7]u8 = [_]u8{0} ** 7,
    created_at_unix_ns: u64 = 0,
    high_seqno: u64 = 0,
    shard_cursors: []ShardCursor,
};

pub const Durability = enum(u8) {
    ultrafast,
    batch,
    strict,
};

pub const RecordType = enum(u8) {
    put_inline,
    put_blob,
    tombstone,
};

pub const IoBackend = enum {
    pwrite,
    io_uring,
};

pub const AppendBatchOptions = struct {
    durability: Durability = .batch,
    await_durable: bool = false,
};

pub const AppendBatchAndCommitResult = struct {
    token: CommitToken,
    window: CommitWindow,
};

pub const EngineConfig = struct {
    shard_count: u16 = 4,
    inline_max: u32 = 1024,
    wal_segment_size: u64 = 256 * 1024 * 1024,
    blob_segment_size: u64 = 1024 * 1024 * 1024,

    batch_sync_interval_ns: u64 = 1_000_000,
    batch_sync_max_dirty_bytes: u64 = 64 * 1024,
    batch_sync_max_dirty_ops: u32 = 1024,
    flusher_sleep_ns: u64 = 250_000,

    io_backend: IoBackend = .pwrite,
    io_uring_entries: u16 = 256,
};

pub const SegmentRef = packed struct {
    segment_id: u32,
    offset: u64,
    len: u32,
};

pub const BlobRef = packed struct {
    segment_id: u32,
    offset: u64,
    total_len: u32,
    value_len: u32,
    checksum: u32,
};

pub const ValueRef = union(enum) {
    @"inline": struct {
        wal: SegmentRef,
        seqno: u64,
    },
    blob: struct {
        blob: BlobRef,
        seqno: u64,
    },
    tombstone: struct {
        seqno: u64,
    },
};

pub const TailState = struct {
    reserved: u64 = 0,
    written: u64 = 0,
    durable: u64 = 0,
};

pub const ActiveSegment = struct {
    id: u32 = 0,
    size_limit: u64,
    tail: TailState = .{},

    pub fn remaining(self: *const ActiveSegment) u64 {
        return self.size_limit - self.tail.reserved;
    }
};

pub const PendingState = enum {
    queued,
    layout_done,
    wal_completed,
    durable,
    acked,
};

pub const PendingOp = struct {
    seqno: u64,
    durability: Durability,
    record_type: RecordType,
    key: []const u8,
    value: []const u8,

    inline_value: bool = false,
    wal_ref: ?SegmentRef = null,
    blob_ref: ?BlobRef = null,
    state: PendingState = .queued,
};

pub const LayoutDecision = union(enum) {
    @"inline": struct {
        wal_len: u32,
    },
    blob: struct {
        blob_len: u32,
        wal_len: u32,
    },
    tombstone: struct {
        wal_len: u32,
    },
};

pub const IndexMap = std.StringHashMapUnmanaged(ValueRef);

pub const EngineStats = struct {
    shard_count: u16,
    visible_offsets: []ShardCursor,
    durable_offsets: []ShardCursor,
};
