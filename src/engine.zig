const std = @import("std");
const types = @import("types.zig");
const shard_mod = @import("shard.zig");
const io_backend = @import("io/backend.zig");

const default_merge_page_size: usize = 256;
const PREFETCH_COUNT: usize = 64;

const ENABLE_DEBUG_LOGS = false;

fn debugLog(comptime fmt: []const u8, args: anytype) void {
    if (!ENABLE_DEBUG_LOGS) return;
    std.debug.print(fmt, args);
}

pub const ReadStats = struct {
    heap_pops: u64 = 0,
    heap_pushes: u64 = 0,

    shard_batches: u64 = 0,
    shard_items_read: u64 = 0,

    merged_iterations: u64 = 0,

    cloned_items: u64 = 0,
    cloned_bytes: u64 = 0,

    pub fn addAssign(self: *ReadStats, other: ReadStats) void {
        self.heap_pops += other.heap_pops;
        self.heap_pushes += other.heap_pushes;
        self.shard_batches += other.shard_batches;
        self.shard_items_read += other.shard_items_read;
        self.merged_iterations += other.merged_iterations;
        self.cloned_items += other.cloned_items;
        self.cloned_bytes += other.cloned_bytes;
    }
};

pub const GlobalMergeReadWithStats = struct {
    result: types.GlobalMergeReadFromResult,
    stats: ReadStats,
};

pub const ReadAllMergedWithStats = struct {
    items: []types.StreamItem,
    stats: ReadStats,
    page_arenas: []std.heap.ArenaAllocator,
};

fn streamItemOwnedBytes(item: types.StreamItem) usize {
    var total: usize = item.key.len;
    switch (item.value) {
        .@"inline" => |v| total += v.len,
        .blob => |v| total += v.len,
        .tombstone => {},
    }
    return total;
}

fn cloneStreamItem(
    allocator: std.mem.Allocator,
    item: types.StreamItem,
) !types.StreamItem {
    const key = try allocator.dupe(u8, item.key);
    errdefer allocator.free(key);

    const value: types.StreamValue = switch (item.value) {
        .@"inline" => |v| .{ .@"inline" = try allocator.dupe(u8, v) },
        .blob => |v| .{ .blob = try allocator.dupe(u8, v) },
        .tombstone => .tombstone,
    };
    errdefer switch (value) {
        .@"inline" => |v| allocator.free(v),
        .blob => |v| allocator.free(v),
        .tombstone => {},
    };

    return .{
        .seqno = item.seqno,
        .key = key,
        .value = value,
        .durability = item.durability,
        .record_type = item.record_type,
        .cursor = item.cursor,
    };
}

fn cloneStreamItemTracked(
    allocator: std.mem.Allocator,
    item: types.StreamItem,
    stats: *ReadStats,
) !types.StreamItem {
    const cloned = try cloneStreamItem(allocator, item);
    stats.cloned_items += 1;
    stats.cloned_bytes += streamItemOwnedBytes(item);
    return cloned;
}

fn freeStreamItem(allocator: std.mem.Allocator, item: types.StreamItem) void {
    allocator.free(item.key);
    switch (item.value) {
        .@"inline" => |v| allocator.free(v),
        .blob => |v| allocator.free(v),
        .tombstone => {},
    }
}

fn freeStreamItems(allocator: std.mem.Allocator, items: []types.StreamItem) void {
    for (items) |item| {
        freeStreamItem(allocator, item);
    }
    allocator.free(items);
}

fn streamItemLessThan(
    a: types.StreamItem,
    a_shard_idx: usize,
    b: types.StreamItem,
    b_shard_idx: usize,
) bool {
    if (a.seqno < b.seqno) return true;
    if (a.seqno > b.seqno) return false;
    return a_shard_idx < b_shard_idx;
}

const ShardPrefetchBuffer = struct {
    items: std.ArrayListUnmanaged(?types.StreamItem) = .{},
    index: usize = 0,

    pub fn deinit(self: *ShardPrefetchBuffer, allocator: std.mem.Allocator) void {
        self.clearAndFreeItems(allocator);
        self.items.deinit(allocator);
    }

    fn clearAndFreeItems(self: *ShardPrefetchBuffer, allocator: std.mem.Allocator) void {
        for (self.items.items) |*slot| {
            if (slot.*) |item| {
                freeStreamItem(allocator, item);
            }
        }
        self.items.clearRetainingCapacity();
        self.index = 0;
    }

    pub fn remaining(self: *const ShardPrefetchBuffer) usize {
        return self.items.items.len - self.index;
    }

    pub fn current(self: *ShardPrefetchBuffer) ?*const types.StreamItem {
        if (self.index >= self.items.items.len) return null;
        const slot = &self.items.items[self.index];
        if (slot.* == null) return null;
        return &slot.*.?;
    }

    pub fn takeCurrent(self: *ShardPrefetchBuffer) ?types.StreamItem {
        if (self.index >= self.items.items.len) return null;
        const value = self.items.items[self.index] orelse return null;
        self.items.items[self.index] = null;
        return value;
    }

    pub fn consumeOne(self: *ShardPrefetchBuffer) void {
        self.index += 1;
        if (self.index >= self.items.items.len) {
            self.items.clearRetainingCapacity();
            self.index = 0;
        }
    }
};

const HeapEntry = struct {
    shard_idx: usize,
};

const ReadSnapshot = struct {
    limits: []types.ShardCursor,
};

const HeapContext = struct {
    buffers: []ShardPrefetchBuffer,

    pub fn lessThan(
        self: @This(),
        a: HeapEntry,
        b: HeapEntry,
    ) std.math.Order {
        const a_item = self.buffers[a.shard_idx].current().?;
        const b_item = self.buffers[b.shard_idx].current().?;

        if (streamItemLessThan(a_item.*, a.shard_idx, b_item.*, b.shard_idx)) {
            return .lt;
        }
        if (streamItemLessThan(b_item.*, b.shard_idx, a_item.*, a.shard_idx)) {
            return .gt;
        }
        return .eq;
    }
};

pub const Engine = struct {
    allocator: std.mem.Allocator,
    config: types.EngineConfig,
    shards: []shard_mod.Shard,

    backend: io_backend.Backend,

    mutex: std.Thread.Mutex = .{},
    flusher_thread: ?std.Thread = null,
    flusher_running: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),

    pub fn init(allocator: std.mem.Allocator, config: types.EngineConfig) !Engine {
        const shard_count: usize = @intCast(config.shard_count);
        const shards = try allocator.alloc(shard_mod.Shard, shard_count);
        errdefer allocator.free(shards);

        var backend = try io_backend.Backend.init(
            switch (config.io_backend) {
                .pwrite => .pwrite,
                .io_uring => .io_uring,
            },
            config.io_uring_entries,
        );
        errdefer backend.deinit();

        for (shards, 0..) |*s, i| {
            s.* = try shard_mod.Shard.init(allocator, @intCast(i), config);
        }

        return .{
            .allocator = allocator,
            .config = config,
            .shards = shards,
            .backend = backend,
        };
    }

    pub fn deinit(self: *Engine) void {
        self.stopFlusher();

        self.mutex.lock();
        defer self.mutex.unlock();

        for (self.shards) |*s| s.deinit();
        self.allocator.free(self.shards);
        self.backend.deinit();
    }

    pub fn captureStats(
        self: *Engine,
        allocator: std.mem.Allocator,
    ) !types.EngineStats {
        self.mutex.lock();
        defer self.mutex.unlock();

        const visible = try allocator.alloc(types.ShardCursor, self.shards.len);
        errdefer allocator.free(visible);

        const durable = try allocator.alloc(types.ShardCursor, self.shards.len);
        errdefer allocator.free(durable);

        for (self.shards, 0..) |*shard, i| {
            visible[i] = shard.readLimit(.visible);
            durable[i] = shard.readLimit(.durable);
        }

        return .{
            .shard_count = @intCast(self.shards.len),
            .visible_offsets = visible,
            .durable_offsets = durable,
        };
    }

    pub fn freeStats(
        allocator: std.mem.Allocator,
        stats: types.EngineStats,
    ) void {
        allocator.free(stats.visible_offsets);
        allocator.free(stats.durable_offsets);
    }

    fn nowMonotonicNs() u64 {
        return @intCast(std.time.nanoTimestamp());
    }

    pub fn startFlusher(self: *Engine) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.flusher_thread != null) return;

        self.flusher_running.store(true, .release);
        self.flusher_thread = try std.Thread.spawn(.{}, flusherMain, .{self});
    }

    pub fn stopFlusher(self: *Engine) void {
        const maybe_thread = blk: {
            self.mutex.lock();
            defer self.mutex.unlock();

            if (self.flusher_thread == null) return;
            self.flusher_running.store(false, .release);

            const t = self.flusher_thread.?;
            self.flusher_thread = null;
            break :blk t;
        };

        maybe_thread.join();
    }

    fn flusherMain(self: *Engine) void {
        while (self.flusher_running.load(.acquire)) {
            self.mutex.lock();
            _ = self.tickLocked(nowMonotonicNs()) catch {};
            self.mutex.unlock();

            std.Thread.sleep(self.config.flusher_sleep_ns);
        }
    }

    pub fn shardForKey(self: *Engine, key: []const u8) *shard_mod.Shard {
        const h = std.hash.Wyhash.hash(0, key);
        const idx = h % self.shards.len;
        return &self.shards[idx];
    }

    pub fn syncNow(self: *Engine) !void {
        try self.syncBatch(nowMonotonicNs());
    }

    pub fn shardIdForKey(self: *Engine, key: []const u8) u16 {
        const h = std.hash.Wyhash.hash(0, key);
        return @intCast(h % self.shards.len);
    }

    pub fn getShardCount(self: *Engine) usize {
        return self.shards.len;
    }

    pub fn appendBatch(
        self: *Engine,
        allocator: std.mem.Allocator,
        durability: types.Durability,
        ops: []const types.BatchOp,
    ) !types.GlobalCommitToken {
        if (ops.len == 0) return error.EmptyBatch;

        self.mutex.lock();
        defer self.mutex.unlock();

        var per_shard_ops = try allocator.alloc(std.ArrayListUnmanaged(types.BatchOp), self.shards.len);
        defer {
            for (per_shard_ops) |*list| {
                list.deinit(allocator);
            }
            allocator.free(per_shard_ops);
        }

        for (per_shard_ops) |*list| {
            list.* = .{};
        }

        for (ops) |op| {
            const key = switch (op) {
                .put => |p| p.key,
                .del => |d| d.key,
            };

            const sid = self.shardIdForKey(key);
            try per_shard_ops[sid].append(allocator, op);
        }

        var tokens = std.ArrayListUnmanaged(types.CommitToken){};
        errdefer tokens.deinit(allocator);

        for (per_shard_ops, 0..) |*list, shard_idx| {
            if (list.items.len == 0) continue;

            const token = try self.shards[shard_idx].enqueueBatch(durability, list.items);
            try tokens.append(allocator, token);
        }

        return .{
            .tokens = try tokens.toOwnedSlice(allocator),
        };
    }

    pub fn appendBatchAndCommit(
        self: *Engine,
        shard_id: u16,
        ops: []const types.BatchOp,
        opts: types.AppendBatchOptions,
    ) !types.AppendBatchAndCommitResult {
        if (ops.len == 0) return error.EmptyBatch;
        if (@as(usize, shard_id) >= self.shards.len) return error.InvalidShardId;

        self.mutex.lock();
        defer self.mutex.unlock();

        const shard = &self.shards[shard_id];
        const pending_start = shard.pending.items.len;

        const token = try shard.enqueueBatch(opts.durability, ops);

        const first_pending = shard.pending.items[pending_start];
        const first_wal_ref = first_pending.wal_ref orelse return error.MissingWalRef;

        const now_ns = nowMonotonicNs();
        try shard.commitPendingAsWritten(now_ns, &self.backend);

        var durable_cursor: ?types.ShardCursor = null;
        if (opts.await_durable or opts.durability == .strict) {
            try shard.syncData(now_ns);
            durable_cursor = shard.readLimit(.durable);
        }

        return .{
            .token = token,
            .window = .{
                .shard_id = shard_id,
                .start = .{
                    .wal_segment_id = first_wal_ref.segment_id,
                    .wal_offset = first_wal_ref.offset,
                },
                .end = .{
                    .wal_segment_id = token.wal_segment_id,
                    .wal_offset = token.wal_offset,
                },
                .visible = shard.readLimit(.visible),
                .durable = durable_cursor,
                .record_count = ops.len,
                .first_seqno = first_pending.seqno,
                .last_seqno = token.seqno,
            },
        };
    }

    pub fn freeGlobalCommitToken(
        allocator: std.mem.Allocator,
        token: types.GlobalCommitToken,
    ) void {
        allocator.free(token.tokens);
    }

    pub fn syncUntilToken(
        self: *Engine,
        token: types.CommitToken,
    ) !void {
        return try self.syncUntil(.{
            .shard_id = token.shard_id,
            .wal_segment_id = token.wal_segment_id,
            .wal_offset = token.wal_offset,
        });
    }

    pub fn syncUntilGlobalToken(
        self: *Engine,
        token: types.GlobalCommitToken,
    ) !void {
        for (token.tokens) |t| {
            try self.syncUntilToken(t);
        }
    }

    pub fn visibleFrontier(
        self: *Engine,
        shard_id: u16,
    ) types.ShardCursor {
        self.mutex.lock();
        defer self.mutex.unlock();

        return self.shards[shard_id].readLimit(.visible);
    }

    pub fn durableFrontier(
        self: *Engine,
        shard_id: u16,
    ) types.ShardCursor {
        self.mutex.lock();
        defer self.mutex.unlock();

        return self.shards[shard_id].readLimit(.durable);
    }

    pub fn put(
        self: *Engine,
        durability: types.Durability,
        key: []const u8,
        value: []const u8,
    ) !void {
        _ = try self.shardForKey(key).enqueuePut(durability, key, value);
    }

    pub fn del(
        self: *Engine,
        durability: types.Durability,
        key: []const u8,
    ) !void {
        _ = try self.shardForKey(key).enqueueDelete(durability, key);
    }

    pub fn get(self: *Engine, key: []const u8) ?types.ValueRef {
        self.mutex.lock();
        defer self.mutex.unlock();
        return self.shardForKey(key).get(key);
    }

    pub fn getRef(self: *Engine, key: []const u8) ?types.ValueRef {
        self.mutex.lock();
        defer self.mutex.unlock();
        return self.shardForKey(key).get(key);
    }

    pub fn compactAll(self: *Engine) !void {
        for (self.shards) |*shard| {
            try shard.compact(self.allocator);
        }
    }

    pub fn compactShard(self: *Engine, shard_id: u16) !void {
        try self.shards[shard_id].compact(self.allocator);
    }

    fn buildReadSnapshot(
        self: *Engine,
        allocator: std.mem.Allocator,
        consistency: types.ReadConsistency,
    ) !ReadSnapshot {
        const limits = try allocator.alloc(types.ShardCursor, self.shards.len);
        for (self.shards, 0..) |*shard, i| {
            limits[i] = shard.readLimit(consistency);
        }
        return .{ .limits = limits };
    }

    pub fn readFromConsistent(
        self: *Engine,
        allocator: std.mem.Allocator,
        cursor: types.Cursor,
        limit: usize,
        consistency: types.ReadConsistency,
    ) !types.ReadFromResult {
        return try self.shards[cursor.shard_id].readFromConsistent(allocator, cursor, limit, consistency);
    }

    pub fn readFrom(
        self: *Engine,
        allocator: std.mem.Allocator,
        cursor: types.Cursor,
        limit: usize,
    ) !types.ReadFromResult {
        return try self.readFromConsistent(allocator, cursor, limit, .visible);
    }

    pub fn getOrNull(
        self: *Engine,
        allocator: std.mem.Allocator,
        key: []const u8,
    ) !?[]u8 {
        return try self.readValue(allocator, key);
    }

    pub fn getAt(
        self: *Engine,
        allocator: std.mem.Allocator,
        cursor: types.Cursor,
        key: []const u8,
    ) !?[]u8 {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (@as(usize, cursor.shard_id) >= self.shards.len) return error.InvalidShardId;
        return try self.shards[cursor.shard_id].getAt(allocator, cursor, key);
    }

    pub fn readFromAll(
        self: *Engine,
        allocator: std.mem.Allocator,
        start: types.GlobalCursor,
        limit: usize,
    ) !types.GlobalReadFromResult {
        return self.readFromAllConsistent(allocator, start, limit, .visible);
    }

    pub fn readFromAllConsistent(
        self: *Engine,
        allocator: std.mem.Allocator,
        start: types.GlobalCursor,
        limit: usize,
        consistency: types.ReadConsistency,
    ) !types.GlobalReadFromResult {
        const snapshot = try self.buildReadSnapshot(allocator, consistency);
        defer allocator.free(snapshot.limits);

        var result_arena = std.heap.ArenaAllocator.init(allocator);
        errdefer result_arena.deinit();

        const result_allocator = result_arena.allocator();

        var temp_items = std.ArrayListUnmanaged(types.StreamItem){};
        errdefer {
            for (temp_items.items) |item| {
                freeStreamItem(allocator, item);
            }
            temp_items.deinit(allocator);
        }

        var shard_id: usize = start.shard_id;
        var seg_id = start.wal_segment_id;
        var off = start.wal_offset;

        while (shard_id < self.shards.len and temp_items.items.len < limit) {
            const next_cursor = try self.shards[shard_id].readFromAppendUntil(
                allocator,
                .{
                    .shard_id = @intCast(shard_id),
                    .wal_segment_id = seg_id,
                    .wal_offset = off,
                },
                limit - temp_items.items.len,
                snapshot.limits[shard_id],
                &temp_items,
            );

            if (temp_items.items.len >= limit) {
                var arena_items = std.ArrayListUnmanaged(types.StreamItem){};
                errdefer {
                    for (arena_items.items) |item| {
                        freeStreamItem(result_allocator, item);
                    }
                    arena_items.deinit(result_allocator);
                }

                try arena_items.ensureTotalCapacity(result_allocator, temp_items.items.len);

                for (temp_items.items) |item| {
                    const cloned = try cloneStreamItem(result_allocator, item);
                    arena_items.appendAssumeCapacity(cloned);
                }

                for (temp_items.items) |item| {
                    freeStreamItem(allocator, item);
                }
                temp_items.deinit(allocator);

                return .{
                    .items = try arena_items.toOwnedSlice(result_allocator),
                    .next_cursor = .{
                        .shard_id = @intCast(shard_id),
                        .wal_segment_id = next_cursor.wal_segment_id,
                        .wal_offset = next_cursor.wal_offset,
                    },
                    .arena = result_arena,
                };
            }

            shard_id += 1;
            seg_id = 0;
            off = 0;
        }

        var arena_items = std.ArrayListUnmanaged(types.StreamItem){};
        errdefer {
            for (arena_items.items) |item| {
                freeStreamItem(result_allocator, item);
            }
            arena_items.deinit(result_allocator);
        }

        try arena_items.ensureTotalCapacity(result_allocator, temp_items.items.len);

        for (temp_items.items) |item| {
            const cloned = try cloneStreamItem(result_allocator, item);
            arena_items.appendAssumeCapacity(cloned);
        }

        for (temp_items.items) |item| {
            freeStreamItem(allocator, item);
        }
        temp_items.deinit(allocator);

        return .{
            .items = try arena_items.toOwnedSlice(result_allocator),
            .next_cursor = .{
                .shard_id = @intCast(shard_id),
                .wal_segment_id = 0,
                .wal_offset = 0,
            },
            .arena = result_arena,
        };
    }

    pub fn cursorForShard(self: *Engine, shard_id: u16) types.Cursor {
        _ = self;
        return .{
            .shard_id = shard_id,
            .wal_segment_id = 0,
            .wal_offset = 0,
        };
    }

    pub fn initGlobalMergeCursor(
        self: *Engine,
        allocator: std.mem.Allocator,
    ) !types.GlobalMergeCursor {
        const cursors = try allocator.alloc(types.ShardCursor, self.shards.len);
        for (cursors) |*c| {
            c.* = .{
                .wal_segment_id = 0,
                .wal_offset = 0,
            };
        }
        return .{
            .shard_cursors = cursors,
        };
    }

    fn refillShardPrefetchBuffer(
        self: *Engine,
        allocator: std.mem.Allocator,
        shard_idx: usize,
        cursor: *types.ShardCursor,
        limit: types.ShardCursor,
        buffer: *ShardPrefetchBuffer,
        max_items: usize,
        stats: ?*ReadStats,
    ) !void {
        if (buffer.remaining() > 0) return;

        const fetch_n = @min(self.effectivePrefetchCount(), max_items);
        if (fetch_n == 0) return;

        buffer.clearAndFreeItems(allocator);

        var temp = std.ArrayListUnmanaged(types.StreamItem){};
        defer {
            for (temp.items) |item| {
                freeStreamItem(allocator, item);
            }
            temp.deinit(allocator);
        }

        const next_cursor = try self.shards[shard_idx].readFromAppendUntil(
            allocator,
            .{
                .shard_id = @intCast(shard_idx),
                .wal_segment_id = cursor.wal_segment_id,
                .wal_offset = cursor.wal_offset,
            },
            fetch_n,
            limit,
            &temp,
        );

        cursor.* = .{
            .wal_segment_id = next_cursor.wal_segment_id,
            .wal_offset = next_cursor.wal_offset,
        };

        if (stats) |s| {
            s.shard_batches += 1;
            s.shard_items_read += @intCast(temp.items.len);
        }

        try buffer.items.ensureTotalCapacity(allocator, temp.items.len);
        for (temp.items) |item| {
            buffer.items.appendAssumeCapacity(item);
        }

        temp.clearRetainingCapacity();
    }

    fn prefetchBudget(remaining_global: usize, active_shards: usize) usize {
        if (remaining_global == 0 or active_shards == 0) return 0;
        const fair_share = std.math.divCeil(usize, remaining_global, active_shards) catch remaining_global;
        return fair_share;
    }

    pub fn readFromAllMerged(
        self: *Engine,
        allocator: std.mem.Allocator,
        start: types.GlobalMergeCursor,
        limit: usize,
    ) !types.GlobalMergeReadFromResult {
        const wrapped = try self.readFromAllMergedWithStats(allocator, start, limit);
        return wrapped.result;
    }

    pub fn readFromAllMergedWithStats(
        self: *Engine,
        allocator: std.mem.Allocator,
        start: types.GlobalMergeCursor,
        limit: usize,
    ) !GlobalMergeReadWithStats {
        return self.readFromAllMergedConsistentWithStats(allocator, start, limit, .visible);
    }

    pub fn readFromAllMergedConsistent(
        self: *Engine,
        allocator: std.mem.Allocator,
        start: types.GlobalMergeCursor,
        limit: usize,
        consistency: types.ReadConsistency,
    ) !types.GlobalMergeReadFromResult {
        const wrapped = try self.readFromAllMergedConsistentWithStats(allocator, start, limit, consistency);
        return wrapped.result;
    }

    pub fn readFromAllMergedConsistentWithStats(
        self: *Engine,
        allocator: std.mem.Allocator,
        start: types.GlobalMergeCursor,
        limit: usize,
        consistency: types.ReadConsistency,
    ) !GlobalMergeReadWithStats {
        var stats = ReadStats{};

        if (limit == 0) {
            return .{
                .result = .{
                    .items = try allocator.alloc(types.StreamItem, 0),
                    .next_cursor = .{
                        .shard_cursors = try allocator.dupe(types.ShardCursor, start.shard_cursors),
                    },
                    .arena = null,
                },
                .stats = stats,
            };
        }

        if (start.shard_cursors.len != self.shards.len) {
            return error.InvalidGlobalMergeCursor;
        }

        const snapshot = try self.buildReadSnapshot(allocator, consistency);
        defer allocator.free(snapshot.limits);

        var result_arena = std.heap.ArenaAllocator.init(allocator);
        errdefer result_arena.deinit();

        const result_allocator = result_arena.allocator();

        var items = std.ArrayListUnmanaged(types.StreamItem){};
        errdefer {
            for (items.items) |item| {
                freeStreamItem(result_allocator, item);
            }
            items.deinit(result_allocator);
        }

        var current_cursors = try allocator.dupe(types.ShardCursor, start.shard_cursors);
        errdefer allocator.free(current_cursors);

        var prefetch_cursors = try allocator.dupe(types.ShardCursor, start.shard_cursors);
        defer allocator.free(prefetch_cursors);

        var buffers = try allocator.alloc(ShardPrefetchBuffer, self.shards.len);
        defer {
            for (buffers) |*buffer| {
                buffer.deinit(allocator);
            }
            allocator.free(buffers);
        }

        for (buffers) |*buffer| {
            buffer.* = .{};
        }

        var heap = std.PriorityQueue(HeapEntry, HeapContext, HeapContext.lessThan).init(
            allocator,
            .{ .buffers = buffers },
        );
        defer heap.deinit();

        var shard_idx: usize = 0;
        while (shard_idx < self.shards.len) : (shard_idx += 1) {
            const remaining_shards = self.shards.len - shard_idx;
            const initial_budget = @min(
                self.effectivePrefetchCount(),
                prefetchBudget(limit - items.items.len, remaining_shards),
            );

            try self.refillShardPrefetchBuffer(
                allocator,
                shard_idx,
                &prefetch_cursors[shard_idx],
                snapshot.limits[shard_idx],
                &buffers[shard_idx],
                initial_budget,
                &stats,
            );

            if (buffers[shard_idx].current() != null) {
                try heap.add(.{ .shard_idx = shard_idx });
                stats.heap_pushes += 1;
            }
        }

        if (heap.count() == 0) {
            return .{
                .result = .{
                    .items = try items.toOwnedSlice(result_allocator),
                    .next_cursor = .{
                        .shard_cursors = current_cursors,
                    },
                    .arena = result_arena,
                },
                .stats = stats,
            };
        }

        while (items.items.len < limit and heap.count() > 0) {
            stats.merged_iterations += 1;
            stats.heap_pops += 1;

            const entry = heap.remove();
            const chosen_idx = entry.shard_idx;

            const chosen_item = buffers[chosen_idx].takeCurrent() orelse continue;
            defer freeStreamItem(allocator, chosen_item);

            current_cursors[chosen_idx] = .{
                .wal_segment_id = chosen_item.cursor.wal_segment_id,
                .wal_offset = chosen_item.cursor.wal_offset,
            };

            try items.append(result_allocator, try cloneStreamItemTracked(result_allocator, chosen_item, &stats));
            buffers[chosen_idx].consumeOne();

            if (buffers[chosen_idx].current() == null) {
                const remaining_global = limit - items.items.len;
                const active_shards = @max(heap.count() + 1, 1);
                const refill_budget = @min(
                    self.effectivePrefetchCount(),
                    prefetchBudget(remaining_global, active_shards),
                );

                try self.refillShardPrefetchBuffer(
                    allocator,
                    chosen_idx,
                    &prefetch_cursors[chosen_idx],
                    snapshot.limits[chosen_idx],
                    &buffers[chosen_idx],
                    refill_budget,
                    &stats,
                );
            }

            if (buffers[chosen_idx].current() != null) {
                try heap.add(.{ .shard_idx = chosen_idx });
                stats.heap_pushes += 1;
            }
        }

        return .{
            .result = .{
                .items = try items.toOwnedSlice(result_allocator),
                .next_cursor = .{
                    .shard_cursors = current_cursors,
                },
                .arena = result_arena,
            },
            .stats = stats,
        };
    }

    pub fn captureCheckpoint(
        self: *Engine,
        allocator: std.mem.Allocator,
        kind: types.CheckpointKind,
    ) !types.EngineCheckpoint {
        self.mutex.lock();
        defer self.mutex.unlock();

        const cursors = try allocator.alloc(types.ShardCursor, self.shards.len);
        errdefer allocator.free(cursors);

        var high_seqno: u64 = 0;
        for (self.shards, 0..) |*shard, i| {
            cursors[i] = switch (kind) {
                .visible => shard.readLimit(.visible),
                .durable => shard.readLimit(.durable),
            };
            if (shard.next_seqno > 0 and shard.next_seqno - 1 > high_seqno) {
                high_seqno = shard.next_seqno - 1;
            }
        }

        return .{
            .version = 1,
            .shard_count = @intCast(self.shards.len),
            .kind = kind,
            .created_at_unix_ns = @intCast(std.time.nanoTimestamp()),
            .high_seqno = high_seqno,
            .shard_cursors = cursors,
        };
    }

    pub fn validateCheckpoint(
        self: *Engine,
        checkpoint: types.EngineCheckpoint,
    ) !void {
        if (checkpoint.version != 1) return error.UnsupportedCheckpointVersion;
        if (checkpoint.shard_count != @as(u16, @intCast(self.shards.len))) return error.CheckpointShardCountMismatch;
        if (checkpoint.shard_cursors.len != self.shards.len) return error.CheckpointShardCountMismatch;

        for (checkpoint.shard_cursors, 0..) |cursor, i| {
            const shard = &self.shards[i];
            const visible = shard.readLimit(.visible);
            const durable = shard.readLimit(.durable);

            if (durable.wal_segment_id > visible.wal_segment_id) return error.InvalidCheckpointFile;
            if (durable.wal_segment_id == visible.wal_segment_id and durable.wal_offset > visible.wal_offset) {
                return error.InvalidCheckpointFile;
            }

            if (cursor.wal_segment_id > visible.wal_segment_id) return error.InvalidCheckpointFile;
            if (cursor.wal_segment_id == visible.wal_segment_id and cursor.wal_offset > visible.wal_offset) {
                return error.InvalidCheckpointFile;
            }

            if (checkpoint.kind == .durable) {
                if (cursor.wal_segment_id > durable.wal_segment_id) return error.InvalidCheckpointFile;
                if (cursor.wal_segment_id == durable.wal_segment_id and cursor.wal_offset > durable.wal_offset) {
                    return error.InvalidCheckpointFile;
                }
            }
        }
    }

    pub fn checkpointToGlobalMergeCursor(
        self: *Engine,
        allocator: std.mem.Allocator,
        checkpoint: types.EngineCheckpoint,
    ) !types.GlobalMergeCursor {
        try self.validateCheckpoint(checkpoint);
        return .{ .shard_cursors = try allocator.dupe(types.ShardCursor, checkpoint.shard_cursors) };
    }

    pub fn freeCheckpoint(
        allocator: std.mem.Allocator,
        checkpoint: types.EngineCheckpoint,
    ) void {
        allocator.free(checkpoint.shard_cursors);
    }

    fn checkpointChecksum(checkpoint: types.EngineCheckpoint) u32 {
        var hasher = std.hash.Crc32.init();
        var buf4: [4]u8 = undefined;
        var buf2: [2]u8 = undefined;
        var buf8: [8]u8 = undefined;

        std.mem.writeInt(u32, &buf4, checkpoint.version, .little);
        hasher.update(&buf4);
        std.mem.writeInt(u16, &buf2, checkpoint.shard_count, .little);
        hasher.update(&buf2);
        hasher.update(&[_]u8{@intFromEnum(checkpoint.kind)});
        std.mem.writeInt(u64, &buf8, checkpoint.created_at_unix_ns, .little);
        hasher.update(&buf8);
        std.mem.writeInt(u64, &buf8, checkpoint.high_seqno, .little);
        hasher.update(&buf8);
        for (checkpoint.shard_cursors) |cursor| {
            std.mem.writeInt(u32, &buf4, cursor.wal_segment_id, .little);
            hasher.update(&buf4);
            std.mem.writeInt(u64, &buf8, cursor.wal_offset, .little);
            hasher.update(&buf8);
        }
        return hasher.final();
    }

    pub fn writeCheckpointFile(
        self: *Engine,
        allocator: std.mem.Allocator,
        path: []const u8,
        kind: types.CheckpointKind,
    ) !void {
        const checkpoint = try self.captureCheckpoint(allocator, kind);
        defer freeCheckpoint(allocator, checkpoint);
        try writeCheckpointFileFromValue(path, checkpoint);
    }

    pub fn writeCheckpointFileFromValue(
        path: []const u8,
        checkpoint: types.EngineCheckpoint,
    ) !void {
        const file = try std.fs.cwd().createFile(path, .{ .truncate = true });
        defer file.close();

        var buf4: [4]u8 = undefined;
        var buf2: [2]u8 = undefined;
        var buf8: [8]u8 = undefined;

        try file.writeAll("CECP");

        std.mem.writeInt(u32, &buf4, checkpoint.version, .little);
        try file.writeAll(&buf4);

        std.mem.writeInt(u16, &buf2, checkpoint.shard_count, .little);
        try file.writeAll(&buf2);

        try file.writeAll(&[_]u8{@intFromEnum(checkpoint.kind)});
        try file.writeAll(&[_]u8{0});

        std.mem.writeInt(u64, &buf8, checkpoint.created_at_unix_ns, .little);
        try file.writeAll(&buf8);

        std.mem.writeInt(u64, &buf8, checkpoint.high_seqno, .little);
        try file.writeAll(&buf8);

        std.mem.writeInt(u32, &buf4, checkpointChecksum(checkpoint), .little);
        try file.writeAll(&buf4);

        for (checkpoint.shard_cursors) |cursor| {
            std.mem.writeInt(u32, &buf4, cursor.wal_segment_id, .little);
            try file.writeAll(&buf4);

            std.mem.writeInt(u64, &buf8, cursor.wal_offset, .little);
            try file.writeAll(&buf8);
        }
    }

    fn readExact(file: std.fs.File, buf: []u8, comptime invalid_err: anytype) !void {
        try readExactOrInvalid(file, buf, invalid_err);
    }

    pub fn readCheckpointFile(
        allocator: std.mem.Allocator,
        path: []const u8,
    ) !types.EngineCheckpoint {
        const file = try std.fs.cwd().openFile(path, .{});
        defer file.close();

        var magic: [4]u8 = undefined;
        try readExact(file, &magic, error.InvalidCheckpointFile);
        if (!std.mem.eql(u8, &magic, "CECP")) return error.InvalidCheckpointFile;

        var buf4: [4]u8 = undefined;
        var buf2: [2]u8 = undefined;
        var buf8: [8]u8 = undefined;
        var one: [1]u8 = undefined;

        try readExact(file, &buf4, error.InvalidCheckpointFile);
        const version = std.mem.readInt(u32, &buf4, .little);

        try readExact(file, &buf2, error.InvalidCheckpointFile);
        const shard_count = std.mem.readInt(u16, &buf2, .little);

        try readExact(file, &one, error.InvalidCheckpointFile);
        const kind_raw = one[0];

        try readExact(file, &one, error.InvalidCheckpointFile);
        _ = one[0];

        try readExact(file, &buf8, error.InvalidCheckpointFile);
        const created_at_unix_ns = std.mem.readInt(u64, &buf8, .little);

        try readExact(file, &buf8, error.InvalidCheckpointFile);
        const high_seqno = std.mem.readInt(u64, &buf8, .little);

        try readExact(file, &buf4, error.InvalidCheckpointFile);
        const checksum = std.mem.readInt(u32, &buf4, .little);

        const cursors = try allocator.alloc(types.ShardCursor, @as(usize, shard_count));
        errdefer allocator.free(cursors);

        for (cursors) |*cursor| {
            try readExact(file, &buf4, error.InvalidCheckpointFile);
            const wal_segment_id = std.mem.readInt(u32, &buf4, .little);

            try readExact(file, &buf8, error.InvalidCheckpointFile);
            const wal_offset = std.mem.readInt(u64, &buf8, .little);

            cursor.* = .{
                .wal_segment_id = wal_segment_id,
                .wal_offset = wal_offset,
            };
        }

        const checkpoint: types.EngineCheckpoint = .{
            .version = version,
            .shard_count = shard_count,
            .kind = std.meta.intToEnum(types.CheckpointKind, kind_raw) catch return error.InvalidCheckpointFile,
            .created_at_unix_ns = created_at_unix_ns,
            .high_seqno = high_seqno,
            .shard_cursors = cursors,
        };

        if (checkpoint.version != 1) return error.InvalidCheckpointFile;
        if (checkpoint.shard_cursors.len != checkpoint.shard_count) return error.InvalidCheckpointFile;
        if (checkpointChecksum(checkpoint) != checksum) return error.CheckpointChecksumMismatch;

        return checkpoint;
    }

    fn freeSnapshotEntry(allocator: std.mem.Allocator, entry: types.SnapshotEntry) void {
        allocator.free(entry.key);
        allocator.free(entry.value);
    }

    fn readExactOrInvalid(file: std.fs.File, buf: []u8, invalid_err: anyerror) !void {
        var off: usize = 0;
        while (off < buf.len) {
            const n = file.read(buf[off..]) catch return invalid_err;
            if (n == 0) return invalid_err;
            off += n;
        }
    }

    fn snapshotChecksum(snapshot: types.StateSnapshot) u32 {
        var hasher = std.hash.Crc32.init();
        var buf2: [2]u8 = undefined;
        var buf4: [4]u8 = undefined;
        var buf8: [8]u8 = undefined;

        std.mem.writeInt(u32, &buf4, snapshot.version, .little);
        hasher.update(&buf4);

        std.mem.writeInt(u16, &buf2, snapshot.shard_count, .little);
        hasher.update(&buf2);

        hasher.update(&[_]u8{@intFromEnum(snapshot.kind)});

        std.mem.writeInt(u64, &buf8, snapshot.created_at_unix_ns, .little);
        hasher.update(&buf8);

        std.mem.writeInt(u64, &buf8, snapshot.high_seqno, .little);
        hasher.update(&buf8);

        std.mem.writeInt(u64, &buf8, snapshot.entries.len, .little);
        hasher.update(&buf8);

        for (snapshot.checkpoint.shard_cursors) |cursor| {
            std.mem.writeInt(u32, &buf4, cursor.wal_segment_id, .little);
            hasher.update(&buf4);

            std.mem.writeInt(u64, &buf8, cursor.wal_offset, .little);
            hasher.update(&buf8);
        }

        for (snapshot.shard_meta) |meta| {
            std.mem.writeInt(u16, &buf2, meta.shard_id, .little);
            hasher.update(&buf2);

            std.mem.writeInt(u64, &buf8, meta.next_seqno, .little);
            hasher.update(&buf8);

            std.mem.writeInt(u32, &buf4, meta.visible.wal_segment_id, .little);
            hasher.update(&buf4);
            std.mem.writeInt(u64, &buf8, meta.visible.wal_offset, .little);
            hasher.update(&buf8);

            std.mem.writeInt(u32, &buf4, meta.durable.wal_segment_id, .little);
            hasher.update(&buf4);
            std.mem.writeInt(u64, &buf8, meta.durable.wal_offset, .little);
            hasher.update(&buf8);
        }

        for (snapshot.entries) |entry| {
            std.mem.writeInt(u16, &buf2, entry.shard_id, .little);
            hasher.update(&buf2);

            std.mem.writeInt(u32, &buf4, @intCast(entry.key.len), .little);
            hasher.update(&buf4);

            std.mem.writeInt(u32, &buf4, @intCast(entry.value.len), .little);
            hasher.update(&buf4);

            hasher.update(entry.key);
            hasher.update(entry.value);
        }

        return hasher.final();
    }

    pub fn freeGlobalMergeCursor(
        allocator: std.mem.Allocator,
        cursor: types.GlobalMergeCursor,
    ) void {
        allocator.free(cursor.shard_cursors);
    }

    pub fn freeGlobalReadFromResult(
        allocator: std.mem.Allocator,
        res: types.GlobalReadFromResult,
    ) void {
        if (res.arena) |arena| {
            var owned_arena = arena;
            owned_arena.deinit();
        } else {
            freeStreamItems(allocator, res.items);
        }
    }

    pub fn captureStateSnapshot(
        self: *Engine,
        allocator: std.mem.Allocator,
        kind: types.CheckpointKind,
    ) !types.StateSnapshot {
        self.mutex.lock();
        defer self.mutex.unlock();

        const checkpoint_cursors = try allocator.alloc(types.ShardCursor, self.shards.len);
        errdefer allocator.free(checkpoint_cursors);

        var high_seqno: u64 = 0;
        for (self.shards, 0..) |*shard, i| {
            checkpoint_cursors[i] = switch (kind) {
                .visible => shard.readLimit(.visible),
                .durable => shard.readLimit(.durable),
            };
            if (shard.next_seqno > 0 and shard.next_seqno - 1 > high_seqno) {
                high_seqno = shard.next_seqno - 1;
            }
        }

        const checkpoint: types.EngineCheckpoint = .{
            .version = 1,
            .shard_count = @intCast(self.shards.len),
            .kind = kind,
            .created_at_unix_ns = @intCast(std.time.nanoTimestamp()),
            .high_seqno = high_seqno,
            .shard_cursors = checkpoint_cursors,
        };
        errdefer freeCheckpoint(allocator, checkpoint);

        const shard_meta = try allocator.alloc(types.SnapshotShardMeta, self.shards.len);
        errdefer allocator.free(shard_meta);

        var reserve_count: usize = 0;
        for (self.shards, 0..) |*shard, i| {
            shard_meta[i] = .{
                .shard_id = @intCast(i),
                .next_seqno = shard.next_seqno,
                .visible = shard.readLimit(.visible),
                .durable = shard.readLimit(.durable),
            };

            reserve_count += shard.state_index.count();
        }

        var entries = std.ArrayListUnmanaged(types.SnapshotEntry){};
        errdefer {
            for (entries.items) |entry| freeSnapshotEntry(allocator, entry);
            entries.deinit(allocator);
        }
        try entries.ensureTotalCapacity(allocator, reserve_count);

        for (self.shards, 0..) |*shard, shard_idx| {
            var it = shard.state_index.iterator();
            while (it.next()) |kv| {
                const key_slice = kv.key_ptr.*;
                const state_entry = kv.value_ptr.*;

                if (state_entry.is_tombstone) continue;

                const maybe_value = try shard.readValueAtStateIndexEntry(allocator, state_entry);
                if (maybe_value == null) continue;

                const value_bytes = maybe_value.?;
                errdefer allocator.free(value_bytes);

                const key_copy = try allocator.dupe(u8, key_slice);
                errdefer allocator.free(key_copy);

                entries.appendAssumeCapacity(.{
                    .shard_id = @intCast(shard_idx),
                    .key = key_copy,
                    .value = value_bytes,
                });
            }
        }

        return .{
            .version = 1,
            .shard_count = @intCast(self.shards.len),
            .kind = kind,
            .created_at_unix_ns = checkpoint.created_at_unix_ns,
            .high_seqno = checkpoint.high_seqno,
            .checkpoint = checkpoint,
            .shard_meta = shard_meta,
            .entries = try entries.toOwnedSlice(allocator),
        };
    }

    pub fn restoreStateSnapshot(
        self: *Engine,
        allocator: std.mem.Allocator,
        snapshot: types.StateSnapshot,
    ) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (snapshot.shard_count != self.shards.len) {
            return error.InvalidSnapshotFile;
        }

        for (self.shards, 0..) |*shard, i| {
            _ = i;

            shard.wal.close(shard.allocator);
            shard.blob.close(shard.allocator);

            for (shard.pending.items) |op| {
                shard.allocator.free(op.key);
                shard.allocator.free(op.value);
            }
            shard.pending.clearRetainingCapacity();

            {
                var it = shard.index.iterator();
                while (it.next()) |entry| {
                    shard.allocator.free(entry.key_ptr.*);
                }
                shard.index.clearRetainingCapacity();
            }

            {
                var it = shard.state_index.iterator();
                while (it.next()) |entry| {
                    shard.allocator.free(entry.key_ptr.*);
                }
                shard.state_index.clearRetainingCapacity();
            }

            var wal_seg: u32 = 0;
            while (wal_seg <= shard.wal_active.id) : (wal_seg += 1) {
                const wal_path = try std.fmt.allocPrint(allocator, "wal-{d:0>3}{s}", .{
                    shard.id,
                    if (wal_seg == 0) ".log" else "",
                });
                allocator.free(wal_path);

                if (wal_seg == 0) {
                    const p = try std.fmt.allocPrint(allocator, "wal-{d:0>3}.log", .{shard.id});
                    defer allocator.free(p);
                    std.fs.cwd().deleteFile(p) catch {};
                } else {
                    const p = try std.fmt.allocPrint(allocator, "wal-{d:0>3}-{d:0>3}.log", .{ shard.id, wal_seg });
                    defer allocator.free(p);
                    std.fs.cwd().deleteFile(p) catch {};
                }
            }

            var blob_seg: u32 = 0;
            while (blob_seg <= shard.blob_active.id) : (blob_seg += 1) {
                if (blob_seg == 0) {
                    const p = try std.fmt.allocPrint(allocator, "blob-{d:0>3}.log", .{shard.id});
                    defer allocator.free(p);
                    std.fs.cwd().deleteFile(p) catch {};
                } else {
                    const p = try std.fmt.allocPrint(allocator, "blob-{d:0>3}-{d:0>3}.log", .{ shard.id, blob_seg });
                    defer allocator.free(p);
                    std.fs.cwd().deleteFile(p) catch {};
                }
            }

            shard.wal = try @import("wal.zig").Wal.openShardSegment(shard.allocator, shard.id, 0);
            shard.blob = try @import("blob.zig").Blob.openShardSegment(shard.allocator, shard.id, 0);

            shard.wal_active = .{ .id = 0, .size_limit = shard.config.wal_segment_size };
            shard.blob_active = .{ .id = 0, .size_limit = shard.config.blob_segment_size };

            shard.frontier = .{};
            shard.dirty = false;
            shard.dirty_since_ns = 0;
            shard.last_sync_ns = 0;
            shard.strongest_dirty_durability = null;
            shard.dirty_bytes = 0;
            shard.dirty_ops = 0;
            shard.next_seqno = 1;
        }

        for (snapshot.entries) |entry| {
            if (entry.shard_id >= self.shards.len) return error.InvalidSnapshotFile;

            const shard = &self.shards[entry.shard_id];
            _ = try shard.enqueuePut(.strict, entry.key, entry.value);
        }

        const now_ns = nowMonotonicNs();
        for (self.shards) |*shard| {
            try shard.commitPendingAsWritten(now_ns, &self.backend);
            try shard.syncData(now_ns);
            shard.clearAcked();
        }

        for (snapshot.shard_meta) |meta| {
            if (meta.shard_id >= self.shards.len) return error.InvalidSnapshotFile;

            const shard = &self.shards[meta.shard_id];

            shard.next_seqno = @max(shard.next_seqno, meta.next_seqno);
            shard.frontier.visible = meta.visible;
            shard.frontier.durable = meta.durable;

            if (meta.visible.wal_segment_id == shard.wal_active.id) {
                shard.wal_active.tail.written = meta.visible.wal_offset;
                shard.wal_active.tail.reserved = @max(shard.wal_active.tail.reserved, meta.visible.wal_offset);
            }

            if (meta.durable.wal_segment_id == shard.wal_active.id) {
                shard.wal_active.tail.durable = meta.durable.wal_offset;
            }
        }
    }

    pub fn writeStateSnapshotFile(
        self: *Engine,
        allocator: std.mem.Allocator,
        path: []const u8,
        kind: types.CheckpointKind,
    ) !void {
        const snapshot = try self.captureStateSnapshot(allocator, kind);
        defer freeStateSnapshot(allocator, snapshot);

        try writeStateSnapshotFileFromValue(path, snapshot);
    }

    pub fn writeStateSnapshotFileFromValue(
        path: []const u8,
        snapshot: types.StateSnapshot,
    ) !void {
        const file = try std.fs.cwd().createFile(path, .{ .truncate = true });
        defer file.close();

        var write_buf: [64 * 1024]u8 = undefined;
        var file_writer = file.writer(&write_buf);
        const writer = &file_writer.interface;

        const allocator = std.heap.page_allocator;
        const checkpoint_bytes = snapshot.checkpoint.shard_cursors.len * (4 + 8);
        const shard_meta_bytes = snapshot.shard_meta.len * (2 + 8 + 4 + 8 + 4 + 8);
        var entry_bytes: usize = 0;
        for (snapshot.entries) |entry| {
            entry_bytes += 2 + 4 + 4 + entry.key.len + entry.value.len;
        }

        const payload_capacity = checkpoint_bytes + shard_meta_bytes + entry_bytes;
        var payload = try std.ArrayList(u8).initCapacity(allocator, payload_capacity);
        defer payload.deinit(allocator);

        try payload.appendSlice(allocator, "CESS");
        try appendIntLe(u32, allocator, &payload, snapshot.version);
        try appendIntLe(u16, allocator, &payload, snapshot.shard_count);
        try payload.append(allocator, @intFromEnum(snapshot.kind));
        try payload.append(allocator, 0);
        try appendIntLe(u64, allocator, &payload, snapshot.created_at_unix_ns);
        try appendIntLe(u64, allocator, &payload, snapshot.high_seqno);
        try appendIntLe(u64, allocator, &payload, snapshot.entries.len);
        try appendIntLe(u32, allocator, &payload, snapshotChecksum(snapshot));

        for (snapshot.checkpoint.shard_cursors) |cursor| {
            try appendIntLe(u32, allocator, &payload, cursor.wal_segment_id);
            try appendIntLe(u64, allocator, &payload, cursor.wal_offset);
        }

        for (snapshot.shard_meta) |meta| {
            try appendIntLe(u16, allocator, &payload, meta.shard_id);
            try appendIntLe(u64, allocator, &payload, meta.next_seqno);
            try appendIntLe(u32, allocator, &payload, meta.visible.wal_segment_id);
            try appendIntLe(u64, allocator, &payload, meta.visible.wal_offset);
            try appendIntLe(u32, allocator, &payload, meta.durable.wal_segment_id);
            try appendIntLe(u64, allocator, &payload, meta.durable.wal_offset);
        }

        for (snapshot.entries) |entry| {
            try appendIntLe(u16, allocator, &payload, entry.shard_id);
            try appendIntLe(u32, allocator, &payload, @as(u32, @intCast(entry.key.len)));
            try appendIntLe(u32, allocator, &payload, @as(u32, @intCast(entry.value.len)));
            try payload.appendSlice(allocator, entry.key);
            try payload.appendSlice(allocator, entry.value);
        }

        try writer.writeAll(payload.items);
        try writer.flush();
    }

    fn appendIntLe(
        comptime T: type,
        allocator: std.mem.Allocator,
        buffer: *std.ArrayList(u8),
        value: T,
    ) !void {
        var tmp: [@sizeOf(T)]u8 = undefined;
        std.mem.writeInt(T, &tmp, value, .little);
        try buffer.appendSlice(allocator, &tmp);
    }

    pub fn inspectStateSnapshotFile(
        allocator: std.mem.Allocator,
        path: []const u8,
    ) !types.SnapshotInspectInfo {
        _ = allocator;

        const file = try std.fs.cwd().openFile(path, .{});
        defer file.close();

        var magic: [4]u8 = undefined;
        try readExact(file, &magic, error.InvalidSnapshotFile);
        if (!std.mem.eql(u8, &magic, "CESS")) return error.InvalidSnapshotFile;

        var buf2: [2]u8 = undefined;
        var buf4: [4]u8 = undefined;
        var buf8: [8]u8 = undefined;
        var one: [1]u8 = undefined;

        try readExact(file, &buf4, error.InvalidSnapshotFile);
        const version = std.mem.readInt(u32, &buf4, .little);

        try readExact(file, &buf2, error.InvalidSnapshotFile);
        const shard_count = std.mem.readInt(u16, &buf2, .little);

        try readExact(file, &one, error.InvalidSnapshotFile);
        const kind_raw = one[0];

        try readExact(file, &one, error.InvalidSnapshotFile);
        _ = one[0];

        try readExact(file, &buf8, error.InvalidSnapshotFile);
        const created_at_unix_ns = std.mem.readInt(u64, &buf8, .little);

        try readExact(file, &buf8, error.InvalidSnapshotFile);
        const high_seqno = std.mem.readInt(u64, &buf8, .little);

        try readExact(file, &buf8, error.InvalidSnapshotFile);
        const entry_count = std.mem.readInt(u64, &buf8, .little);

        try readExact(file, &buf4, error.InvalidSnapshotFile);
        _ = std.mem.readInt(u32, &buf4, .little);

        return .{
            .version = version,
            .shard_count = shard_count,
            .kind = std.meta.intToEnum(types.CheckpointKind, kind_raw) catch return error.InvalidSnapshotFile,
            .created_at_unix_ns = created_at_unix_ns,
            .high_seqno = high_seqno,
            .entry_count = entry_count,
        };
    }

    pub fn readStateSnapshotFile(
        allocator: std.mem.Allocator,
        path: []const u8,
    ) !types.StateSnapshot {
        const file = try std.fs.cwd().openFile(path, .{});
        defer file.close();

        var magic: [4]u8 = undefined;
        try readExact(file, &magic, error.InvalidSnapshotFile);
        if (!std.mem.eql(u8, &magic, "CESS")) return error.InvalidSnapshotFile;

        var buf2: [2]u8 = undefined;
        var buf4: [4]u8 = undefined;
        var buf8: [8]u8 = undefined;
        var one: [1]u8 = undefined;

        try readExact(file, &buf4, error.InvalidSnapshotFile);
        const version = std.mem.readInt(u32, &buf4, .little);

        try readExact(file, &buf2, error.InvalidSnapshotFile);
        const shard_count = std.mem.readInt(u16, &buf2, .little);

        try readExact(file, &one, error.InvalidSnapshotFile);
        const kind_raw = one[0];

        try readExact(file, &one, error.InvalidSnapshotFile);
        _ = one[0];

        try readExact(file, &buf8, error.InvalidSnapshotFile);
        const created_at_unix_ns = std.mem.readInt(u64, &buf8, .little);

        try readExact(file, &buf8, error.InvalidSnapshotFile);
        const high_seqno = std.mem.readInt(u64, &buf8, .little);

        try readExact(file, &buf8, error.InvalidSnapshotFile);
        const entry_count = std.mem.readInt(u64, &buf8, .little);

        try readExact(file, &buf4, error.InvalidSnapshotFile);
        const checksum = std.mem.readInt(u32, &buf4, .little);

        const kind = std.meta.intToEnum(types.CheckpointKind, kind_raw) catch return error.InvalidSnapshotFile;

        const checkpoint_cursors = try allocator.alloc(types.ShardCursor, @intCast(shard_count));
        errdefer allocator.free(checkpoint_cursors);

        for (checkpoint_cursors) |*cursor| {
            try readExact(file, &buf4, error.InvalidSnapshotFile);
            const seg = std.mem.readInt(u32, &buf4, .little);

            try readExact(file, &buf8, error.InvalidSnapshotFile);
            const off = std.mem.readInt(u64, &buf8, .little);

            cursor.* = .{
                .wal_segment_id = seg,
                .wal_offset = off,
            };
        }

        const shard_meta = try allocator.alloc(types.SnapshotShardMeta, @intCast(shard_count));
        errdefer allocator.free(shard_meta);

        for (shard_meta) |*meta| {
            try readExact(file, &buf2, error.InvalidSnapshotFile);
            const shard_id = std.mem.readInt(u16, &buf2, .little);

            try readExact(file, &buf8, error.InvalidSnapshotFile);
            const next_seqno = std.mem.readInt(u64, &buf8, .little);

            try readExact(file, &buf4, error.InvalidSnapshotFile);
            const vis_seg = std.mem.readInt(u32, &buf4, .little);
            try readExact(file, &buf8, error.InvalidSnapshotFile);
            const vis_off = std.mem.readInt(u64, &buf8, .little);

            try readExact(file, &buf4, error.InvalidSnapshotFile);
            const dur_seg = std.mem.readInt(u32, &buf4, .little);
            try readExact(file, &buf8, error.InvalidSnapshotFile);
            const dur_off = std.mem.readInt(u64, &buf8, .little);

            meta.* = .{
                .shard_id = shard_id,
                .next_seqno = next_seqno,
                .visible = .{
                    .wal_segment_id = vis_seg,
                    .wal_offset = vis_off,
                },
                .durable = .{
                    .wal_segment_id = dur_seg,
                    .wal_offset = dur_off,
                },
            };
        }

        const entries = try allocator.alloc(types.SnapshotEntry, @intCast(entry_count));
        errdefer allocator.free(entries);

        var entries_init: usize = 0;
        errdefer {
            for (entries[0..entries_init]) |entry| freeSnapshotEntry(allocator, entry);
        }

        for (entries) |*entry| {
            try readExact(file, &buf2, error.InvalidSnapshotFile);
            const shard_id = std.mem.readInt(u16, &buf2, .little);

            try readExact(file, &buf4, error.InvalidSnapshotFile);
            const key_len = std.mem.readInt(u32, &buf4, .little);

            try readExact(file, &buf4, error.InvalidSnapshotFile);
            const value_len = std.mem.readInt(u32, &buf4, .little);

            const key = try allocator.alloc(u8, key_len);
            errdefer allocator.free(key);
            try readExact(file, key, error.InvalidSnapshotFile);

            const value = try allocator.alloc(u8, value_len);
            errdefer allocator.free(value);
            try readExact(file, value, error.InvalidSnapshotFile);

            entry.* = .{
                .shard_id = shard_id,
                .key = key,
                .value = value,
            };
            entries_init += 1;
        }

        const checkpoint: types.EngineCheckpoint = .{
            .version = version,
            .shard_count = shard_count,
            .kind = kind,
            .created_at_unix_ns = created_at_unix_ns,
            .high_seqno = high_seqno,
            .shard_cursors = checkpoint_cursors,
        };

        const snapshot: types.StateSnapshot = .{
            .version = version,
            .shard_count = shard_count,
            .kind = kind,
            .created_at_unix_ns = created_at_unix_ns,
            .high_seqno = high_seqno,
            .checkpoint = checkpoint,
            .shard_meta = shard_meta,
            .entries = entries,
        };

        for (snapshot.shard_meta, 0..) |meta, i| {
            if (meta.shard_id != i) return error.InvalidSnapshotFile;
            if (meta.durable.wal_segment_id > meta.visible.wal_segment_id) return error.InvalidSnapshotFile;
            if (meta.durable.wal_segment_id == meta.visible.wal_segment_id and meta.durable.wal_offset > meta.visible.wal_offset) {
                return error.InvalidSnapshotFile;
            }
        }

        if (snapshotChecksum(snapshot) != checksum) return error.SnapshotChecksumMismatch;
        return snapshot;
    }

    pub fn freeStateSnapshot(
        allocator: std.mem.Allocator,
        snapshot: types.StateSnapshot,
    ) void {
        for (snapshot.entries) |entry| {
            freeSnapshotEntry(allocator, entry);
        }
        allocator.free(snapshot.entries);
        allocator.free(snapshot.shard_meta);
        freeCheckpoint(allocator, snapshot.checkpoint);
    }

    pub fn freeGlobalMergeReadFromResult(
        allocator: std.mem.Allocator,
        res: types.GlobalMergeReadFromResult,
    ) void {
        allocator.free(res.next_cursor.shard_cursors);

        if (res.arena) |arena| {
            var owned_arena = arena;
            owned_arena.deinit();
        } else {
            freeStreamItems(allocator, res.items);
        }
    }

    pub fn readValue(
        self: *Engine,
        allocator: std.mem.Allocator,
        key: []const u8,
    ) !?[]u8 {
        self.mutex.lock();
        defer self.mutex.unlock();
        return try self.shardForKey(key).readValue(allocator, key);
    }

    pub fn readAllMerged(
        self: *Engine,
        allocator: std.mem.Allocator,
    ) ![]types.StreamItem {
        const wrapped = try self.readAllMergedPageSizedWithStats(allocator, default_merge_page_size);
        defer self.freeReadAllMergedWithStats(allocator, wrapped);

        var out = std.ArrayListUnmanaged(types.StreamItem){};
        errdefer {
            for (out.items) |item| {
                freeStreamItem(allocator, item);
            }
            out.deinit(allocator);
        }

        try out.ensureTotalCapacity(allocator, wrapped.items.len);
        for (wrapped.items) |item| {
            out.appendAssumeCapacity(try cloneStreamItem(allocator, item));
        }

        return try out.toOwnedSlice(allocator);
    }

    pub fn readAllMergedPageSized(
        self: *Engine,
        allocator: std.mem.Allocator,
        page_size: usize,
    ) ![]types.StreamItem {
        const wrapped = try self.readAllMergedPageSizedWithStats(allocator, page_size);
        defer self.freeReadAllMergedWithStats(allocator, wrapped);

        var out = std.ArrayListUnmanaged(types.StreamItem){};
        errdefer {
            for (out.items) |item| {
                freeStreamItem(allocator, item);
            }
            out.deinit(allocator);
        }

        try out.ensureTotalCapacity(allocator, wrapped.items.len);
        for (wrapped.items) |item| {
            out.appendAssumeCapacity(try cloneStreamItem(allocator, item));
        }

        return try out.toOwnedSlice(allocator);
    }

    pub fn readAllMergedPageSizedWithStats(
        self: *Engine,
        allocator: std.mem.Allocator,
        page_size: usize,
    ) !ReadAllMergedWithStats {
        var out = std.ArrayListUnmanaged(types.StreamItem){};
        errdefer {
            allocator.free(out.items);
        }

        var arenas = std.ArrayListUnmanaged(std.heap.ArenaAllocator){};
        errdefer {
            for (arenas.items) |*arena| {
                arena.deinit();
            }
            arenas.deinit(allocator);
        }

        var total_stats = ReadStats{};

        var cursor = try self.initGlobalMergeCursor(allocator);
        errdefer freeGlobalMergeCursor(allocator, cursor);

        while (true) {
            const page = try self.readFromAllMergedWithStats(allocator, cursor, page_size);
            total_stats.addAssign(page.stats);

            freeGlobalMergeCursor(allocator, cursor);

            const next_cursor_copy = try allocator.dupe(
                types.ShardCursor,
                page.result.next_cursor.shard_cursors,
            );

            if (page.result.items.len == 0) {
                allocator.free(page.result.next_cursor.shard_cursors);

                if (page.result.arena) |arena| {
                    var owned_arena = arena;
                    owned_arena.deinit();
                }

                cursor = .{ .shard_cursors = next_cursor_copy };
                break;
            }

            try out.ensureTotalCapacity(allocator, out.items.len + page.result.items.len);
            for (page.result.items) |item| {
                out.appendAssumeCapacity(item);
            }

            allocator.free(page.result.next_cursor.shard_cursors);

            if (page.result.arena) |arena| {
                try arenas.append(allocator, arena);
            }

            cursor = .{
                .shard_cursors = next_cursor_copy,
            };
        }

        freeGlobalMergeCursor(allocator, cursor);

        return .{
            .items = try out.toOwnedSlice(allocator),
            .stats = total_stats,
            .page_arenas = try arenas.toOwnedSlice(allocator),
        };
    }

    pub fn freeReadAllMergedWithStats(
        self: *Engine,
        allocator: std.mem.Allocator,
        res: ReadAllMergedWithStats,
    ) void {
        _ = self;

        allocator.free(res.items);

        for (res.page_arenas) |*arena| {
            arena.deinit();
        }
        allocator.free(res.page_arenas);
    }

    pub fn freeMergedItems(
        allocator: std.mem.Allocator,
        items: []types.StreamItem,
    ) void {
        freeStreamItems(allocator, items);
    }

    fn shardCursorAtOrPast(current: types.ShardCursor, target: types.ShardCursor) bool {
        if (current.wal_segment_id > target.wal_segment_id) return true;
        if (current.wal_segment_id < target.wal_segment_id) return false;
        return current.wal_offset >= target.wal_offset;
    }

    pub fn syncUntil(self: *Engine, target: types.Cursor) !void {
        while (true) {
            self.mutex.lock();
            const shard = &self.shards[target.shard_id];
            if (shardCursorAtOrPast(shard.readLimit(.durable), .{
                .wal_segment_id = target.wal_segment_id,
                .wal_offset = target.wal_offset,
            })) {
                self.mutex.unlock();
                return;
            }

            try shard.syncData(nowMonotonicNs());
            self.mutex.unlock();
            std.Thread.sleep(50_000);
        }
    }

    pub fn flushAllWritten(self: *Engine, now_ns: u64) !void {
        self.mutex.lock();
        defer self.mutex.unlock();
        try self.flushAllWrittenLocked(now_ns);
    }

    fn flushAllWrittenLocked(self: *Engine, now_ns: u64) !void {
        for (self.shards) |*s| {
            try s.commitPendingAsWritten(now_ns, &self.backend);
        }
    }

    pub fn markAllDurable(self: *Engine) void {
        self.mutex.lock();
        defer self.mutex.unlock();
        for (self.shards) |*s| s.markDurable();
    }

    fn effectivePrefetchCount(self: *Engine) usize {
        const shard_count = self.shards.len;

        if (shard_count <= 2) return PREFETCH_COUNT; // 64
        if (shard_count <= 4) return PREFETCH_COUNT / 2; // 32
        return @max(@as(usize, 8), PREFETCH_COUNT / 4); // 16
    }

    pub fn clearAllAcked(self: *Engine) void {
        self.mutex.lock();
        defer self.mutex.unlock();
        for (self.shards) |*s| s.clearAcked();
    }

    pub fn syncBatch(self: *Engine, now_ns: u64) !void {
        self.mutex.lock();
        defer self.mutex.unlock();
        try self.syncBatchLocked(now_ns);
    }

    fn syncBatchLocked(self: *Engine, now_ns: u64) !void {
        for (self.shards) |*s| {
            try s.syncData(now_ns);
        }
    }

    pub fn commit(self: *Engine) !void {
        try self.commitPending();
    }

    pub fn commitPending(self: *Engine) !void {
        self.mutex.lock();
        defer self.mutex.unlock();
        try self.commitPendingLocked();
    }

    fn commitPendingLocked(self: *Engine) !void {
        const now_ns = nowMonotonicNs();

        var strongest: ?types.Durability = null;

        for (self.shards) |*s| {
            const shard_durability = s.strongestPendingDurability();
            if (shard_durability) |d| {
                if (strongest == null or @intFromEnum(d) > @intFromEnum(strongest.?)) {
                    strongest = d;
                }
            }
        }

        try self.flushAllWrittenLocked(now_ns);

        if (strongest) |d| {
            switch (d) {
                .ultrafast => {},
                .batch => {},
                .strict => try self.syncBatchLocked(now_ns),
            }
        }
    }

    pub fn tick(self: *Engine, now_ns: u64) !void {
        self.mutex.lock();
        defer self.mutex.unlock();
        try self.tickLocked(now_ns);
    }

    fn tickLocked(self: *Engine, now_ns: u64) !void {
        for (self.shards) |*s| {
            if (!s.dirty) continue;

            const strongest = s.strongest_dirty_durability orelse continue;

            switch (strongest) {
                .ultrafast => {},
                .batch, .strict => {
                    if (now_ns < s.dirty_since_ns) continue;

                    const elapsed = now_ns - s.dirty_since_ns;
                    const hit_time = elapsed >= self.config.batch_sync_interval_ns;
                    const hit_bytes = s.dirty_bytes >= self.config.batch_sync_max_dirty_bytes;
                    const hit_ops = s.dirty_ops >= self.config.batch_sync_max_dirty_ops;

                    if (hit_time or hit_bytes or hit_ops) {
                        try s.syncData(now_ns);
                    }
                },
            }
        }
    }
};

test "engine put/get basic flow" {
    var i: usize = 0;
    while (i < 8) : (i += 1) {
        var wal_name_buf: [32]u8 = undefined;
        var blob_name_buf: [32]u8 = undefined;

        const wal_name = std.fmt.bufPrint(&wal_name_buf, "wal-{d:0>3}.log", .{i}) catch unreachable;
        const blob_name = std.fmt.bufPrint(&blob_name_buf, "blob-{d:0>3}.log", .{i}) catch unreachable;

        std.fs.cwd().deleteFile(wal_name) catch {};
        std.fs.cwd().deleteFile(blob_name) catch {};
    }

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();

    const allocator = gpa.allocator();

    var e = try Engine.init(allocator, .{
        .shard_count = 1,
        .inline_max = 8,
    });
    defer e.deinit();

    _ = try e.put(.ultrafast, "user:1", "hello");
    try e.commitPending();

    const found = e.get("user:1");
    try std.testing.expect(found != null);
}
