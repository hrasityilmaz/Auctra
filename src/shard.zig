const std = @import("std");
const types = @import("types.zig");
const record = @import("record.zig");
const wal_mod = @import("wal.zig");
const blob_mod = @import("blob.zig");
const recovery = @import("recovery.zig");
const io_backend = @import("io/backend.zig");

const LiveEntry = struct {
    key: []const u8,
    value: types.ValueRef,
};

const StateIndexEntry = struct {
    wal_segment_id: u32,
    wal_offset: u64,
    seqno: u64,
    is_tombstone: bool,
};

const OpenedSegmentFile = struct {
    segment_id: u32,
    file: std.fs.File,
};

fn allocWalSegmentPath(
    allocator: std.mem.Allocator,
    shard_id: u16,
    segment_id: u32,
) ![]u8 {
    if (segment_id == 0) {
        return try std.fmt.allocPrint(allocator, "wal-{d:0>3}.log", .{shard_id});
    }
    return try std.fmt.allocPrint(
        allocator,
        "wal-{d:0>3}-{d:0>3}.log",
        .{ shard_id, segment_id },
    );
}

fn allocBlobSegmentPath(
    allocator: std.mem.Allocator,
    shard_id: u16,
    segment_id: u32,
) ![]u8 {
    if (segment_id == 0) {
        return try std.fmt.allocPrint(allocator, "blob-{d:0>3}.log", .{shard_id});
    }
    return try std.fmt.allocPrint(
        allocator,
        "blob-{d:0>3}-{d:0>3}.log",
        .{ shard_id, segment_id },
    );
}

pub const Shard = struct {
    allocator: std.mem.Allocator,
    id: u16,
    config: types.EngineConfig,

    wal: wal_mod.Wal,
    blob: blob_mod.Blob,

    wal_active: types.ActiveSegment,
    blob_active: types.ActiveSegment,
    next_seqno: u64 = 1,
    frontier: types.FrontierWindow = .{},

    dirty: bool = false,
    dirty_since_ns: u64 = 0,
    last_sync_ns: u64 = 0,
    strongest_dirty_durability: ?types.Durability = null,
    dirty_bytes: u64 = 0,
    dirty_ops: u32 = 0,

    index: types.IndexMap = .{},
    pending: std.ArrayListUnmanaged(types.PendingOp) = .{},

    state_index: std.StringHashMapUnmanaged(StateIndexEntry) = .{},

    pub fn init(allocator: std.mem.Allocator, id: u16, config: types.EngineConfig) !Shard {
        var shard = Shard{
            .allocator = allocator,
            .id = id,
            .config = config,
            .wal = undefined,
            .blob = undefined,
            .wal_active = .{ .id = 0, .size_limit = config.wal_segment_size },
            .blob_active = .{ .id = 0, .size_limit = config.blob_segment_size },
        };

        const replay = try recovery.replayShardSegments(
            allocator,
            id,
            &shard.index,
        );

        shard.wal = try wal_mod.Wal.openShardSegment(allocator, id, replay.wal_segment_id);
        errdefer shard.wal.close(allocator);

        shard.blob = try blob_mod.Blob.openShardSegment(allocator, id, replay.blob_segment_id);
        errdefer shard.blob.close(allocator);

        try shard.wal.file.setEndPos(replay.wal_end);
        try shard.blob.file.setEndPos(replay.blob_end);

        shard.wal_active.id = replay.wal_segment_id;
        shard.wal_active.tail.reserved = replay.wal_end;
        shard.wal_active.tail.written = replay.wal_end;
        shard.wal_active.tail.durable = replay.wal_end;
        shard.frontier.visible = .{
            .wal_segment_id = replay.wal_segment_id,
            .wal_offset = replay.wal_end,
        };
        shard.frontier.durable = shard.frontier.visible;

        shard.blob_active.id = replay.blob_segment_id;
        shard.blob_active.tail.reserved = replay.blob_end;
        shard.blob_active.tail.written = replay.blob_end;
        shard.blob_active.tail.durable = replay.blob_end;

        shard.next_seqno = replay.next_seqno;
        try shard.rebuildStateIndexFromWal(allocator);

        return shard;
    }

    pub fn deinit(self: *Shard) void {
        for (self.pending.items) |op| {
            self.allocator.free(op.key);
            self.allocator.free(op.value);
        }
        self.pending.deinit(self.allocator);

        var it = self.index.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
        }
        self.index.deinit(self.allocator);

        self.wal.close(self.allocator);
        self.blob.close(self.allocator);
        self.deinitStateIndex(self.allocator);
    }

    pub fn enqueueBatch(
        self: *Shard,
        durability: types.Durability,
        ops: []const types.BatchOp,
    ) !types.CommitToken {
        if (ops.len == 0) return error.EmptyBatch;

        var last_token: ?types.CommitToken = null;

        for (ops) |op| {
            switch (op) {
                .put => |p| {
                    const seqno = try self.enqueuePut(durability, p.key, p.value);
                    const pending_op = &self.pending.items[self.pending.items.len - 1];
                    const wal_ref = pending_op.wal_ref orelse return error.MissingWalRef;

                    last_token = .{
                        .shard_id = self.id,
                        .wal_segment_id = wal_ref.segment_id,
                        .wal_offset = wal_ref.offset + wal_ref.len,
                        .seqno = seqno,
                    };
                },
                .del => |d| {
                    const seqno = try self.enqueueDelete(durability, d.key);
                    const pending_op = &self.pending.items[self.pending.items.len - 1];
                    const wal_ref = pending_op.wal_ref orelse return error.MissingWalRef;

                    last_token = .{
                        .shard_id = self.id,
                        .wal_segment_id = wal_ref.segment_id,
                        .wal_offset = wal_ref.offset + wal_ref.len,
                        .seqno = seqno,
                    };
                },
            }
        }

        return last_token.?;
    }

    fn deinitStateIndex(self: *Shard, allocator: std.mem.Allocator) void {
        var it = self.state_index.iterator();
        while (it.next()) |entry| {
            allocator.free(entry.key_ptr.*);
        }
        self.state_index.deinit(allocator);
    }

    fn clearStateIndex(self: *Shard, allocator: std.mem.Allocator) void {
        var it = self.state_index.iterator();
        while (it.next()) |entry| {
            allocator.free(entry.key_ptr.*);
        }
        self.state_index.clearRetainingCapacity();
    }

    fn applyRecordToStateIndex(
        self: *Shard,
        allocator: std.mem.Allocator,
        item: types.StreamItem,
    ) !void {
        try self.upsertStateIndex(allocator, item.key, .{
            .wal_segment_id = item.cursor.wal_segment_id,
            .wal_offset = item.cursor.wal_offset - recordSizeFromItemCursor(item),
            .seqno = item.seqno,
            .is_tombstone = switch (item.value) {
                .tombstone => true,
                else => false,
            },
        });
    }

    fn recordSizeFromItemCursor(item: types.StreamItem) u64 {
        return switch (item.value) {
            .@"inline" => @as(u64, record.inlineWalSize(item.key.len, item.value.@"inline".len)),
            .blob => @as(u64, record.blobWalSize(item.key.len)),
            .tombstone => @as(u64, record.tombstoneWalSize(item.key.len)),
        };
    }

    fn rebuildStateIndexFromWal(self: *Shard, allocator: std.mem.Allocator) !void {
        self.clearStateIndex(allocator);

        var cursor = types.Cursor{
            .shard_id = self.id,
            .wal_segment_id = 0,
            .wal_offset = 0,
        };

        while (true) {
            var items = std.ArrayListUnmanaged(types.StreamItem){};
            defer {
                for (items.items) |item| {
                    freeStreamItem(allocator, item);
                }
                items.deinit(allocator);
            }

            const next_cursor = try self.readFromAppendUntil(
                allocator,
                cursor,
                256,
                self.frontier.visible,
                &items,
            );

            for (items.items) |item| {
                try self.applyRecordToStateIndex(allocator, item);
            }

            if (items.items.len == 0) break;
            cursor = next_cursor;
        }
    }

    fn upsertStateIndex(
        self: *Shard,
        allocator: std.mem.Allocator,
        key: []const u8,
        entry: StateIndexEntry,
    ) !void {
        const gop = try self.state_index.getOrPut(allocator, key);

        if (!gop.found_existing) {
            errdefer _ = self.state_index.remove(key);

            const owned_key = try allocator.dupe(u8, key);
            gop.key_ptr.* = owned_key;
        }

        gop.value_ptr.* = entry;
    }

    fn openWalSegmentForRead(self: *Shard, segment_id: u32) !std.fs.File {
        const path = try allocWalSegmentPath(self.allocator, self.id, segment_id);
        defer self.allocator.free(path);
        return try std.fs.cwd().openFile(path, .{ .mode = .read_only });
    }

    fn openWalSegmentForWrite(self: *Shard, segment_id: u32) !std.fs.File {
        const path = try allocWalSegmentPath(self.allocator, self.id, segment_id);
        defer self.allocator.free(path);
        return try std.fs.cwd().openFile(path, .{ .mode = .read_write });
    }

    fn openBlobSegmentForRead(self: *Shard, segment_id: u32) !std.fs.File {
        const path = try allocBlobSegmentPath(self.allocator, self.id, segment_id);
        defer self.allocator.free(path);
        return try std.fs.cwd().openFile(path, .{ .mode = .read_only });
    }

    fn openBlobSegmentForWrite(self: *Shard, segment_id: u32) !std.fs.File {
        const path = try allocBlobSegmentPath(self.allocator, self.id, segment_id);
        defer self.allocator.free(path);
        return try std.fs.cwd().openFile(path, .{ .mode = .read_write });
    }

    fn walFileForSegment(
        self: *Shard,
        opened: *std.ArrayListUnmanaged(OpenedSegmentFile),
        segment_id: u32,
    ) !*std.fs.File {
        if (segment_id == self.wal_active.id) return &self.wal.file;

        for (opened.items) |*item| {
            if (item.segment_id == segment_id) return &item.file;
        }

        const f = try self.openWalSegmentForWrite(segment_id);
        try opened.append(self.allocator, .{
            .segment_id = segment_id,
            .file = f,
        });
        return &opened.items[opened.items.len - 1].file;
    }

    fn blobFileForSegment(
        self: *Shard,
        opened: *std.ArrayListUnmanaged(OpenedSegmentFile),
        segment_id: u32,
    ) !*std.fs.File {
        if (segment_id == self.blob_active.id) return &self.blob.file;

        for (opened.items) |*item| {
            if (item.segment_id == segment_id) return &item.file;
        }

        const f = try self.openBlobSegmentForWrite(segment_id);
        try opened.append(self.allocator, .{
            .segment_id = segment_id,
            .file = f,
        });
        return &opened.items[opened.items.len - 1].file;
    }

    fn collectLiveEntries(
        self: *Shard,
        allocator: std.mem.Allocator,
    ) !std.ArrayListUnmanaged(LiveEntry) {
        var list: std.ArrayListUnmanaged(LiveEntry) = .{};
        errdefer {
            for (list.items) |item| allocator.free(item.key);
            list.deinit(allocator);
        }

        var it = self.index.iterator();
        while (it.next()) |entry| {
            const key_copy = try allocator.dupe(u8, entry.key_ptr.*);

            switch (entry.value_ptr.*) {
                .@"inline", .blob => {
                    try list.append(allocator, .{
                        .key = key_copy,
                        .value = entry.value_ptr.*,
                    });
                },
                .tombstone => allocator.free(key_copy),
            }
        }

        return list;
    }

    fn clearIndexAndFreeKeys(
        self: *Shard,
        allocator: std.mem.Allocator,
    ) void {
        var it = self.index.iterator();
        while (it.next()) |entry| {
            allocator.free(entry.key_ptr.*);
        }
        self.index.clearRetainingCapacity();
    }

    fn readInlineValueAt(
        self: *Shard,
        allocator: std.mem.Allocator,
        wal_ref: types.SegmentRef,
    ) ![]u8 {
        var maybe_file: ?std.fs.File = null;
        defer if (maybe_file) |*f| f.close();

        const file = if (wal_ref.segment_id == self.wal_active.id)
            self.wal.file
        else blk: {
            maybe_file = try self.openWalSegmentForRead(@intCast(wal_ref.segment_id));
            break :blk maybe_file.?;
        };

        const total_len: usize = @intCast(wal_ref.len);
        var buf = try allocator.alloc(u8, total_len);
        defer allocator.free(buf);

        const n = try file.pread(buf, wal_ref.offset);
        if (n != total_len) return error.UnexpectedEof;
        if (buf.len < @sizeOf(record.RecordHeader)) return error.InvalidRecord;

        const header = std.mem.bytesToValue(
            record.RecordHeader,
            buf[0..@sizeOf(record.RecordHeader)],
        );

        if (header.magic != record.RecordMagic) return error.InvalidMagic;
        if (header.total_len != wal_ref.len) return error.LengthMismatch;

        const key_len: usize = @intCast(header.key_len);
        const value_len: usize = @intCast(header.value_len);

        const value_start = @sizeOf(record.RecordHeader) + key_len;
        const value_end = value_start + value_len;
        if (value_end + 4 > buf.len) return error.InvalidRecord;

        const payload = buf[@sizeOf(record.RecordHeader)..];
        if (record.crc32(payload) != header.checksum) return error.ChecksumMismatch;

        return try allocator.dupe(u8, buf[value_start..value_end]);
    }

    fn hasIncompletePending(self: *Shard) bool {
        for (self.pending.items) |op| {
            switch (op.state) {
                .queued, .layout_done, .wal_completed => return true,
                .durable, .acked => {},
            }
        }
        return false;
    }

    fn deleteAllSegments(self: *Shard) !void {
        const cwd = std.fs.cwd();

        var wal_seg: u32 = 0;
        while (wal_seg <= self.wal_active.id) : (wal_seg += 1) {
            const path = try allocWalSegmentPath(self.allocator, self.id, wal_seg);
            defer self.allocator.free(path);
            cwd.deleteFile(path) catch |err| switch (err) {
                error.FileNotFound => {},
                else => return err,
            };
        }

        var blob_seg: u32 = 0;
        while (blob_seg <= self.blob_active.id) : (blob_seg += 1) {
            const path = try allocBlobSegmentPath(self.allocator, self.id, blob_seg);
            defer self.allocator.free(path);
            cwd.deleteFile(path) catch |err| switch (err) {
                error.FileNotFound => {},
                else => return err,
            };
        }
    }

    pub fn compact(
        self: *Shard,
        allocator: std.mem.Allocator,
    ) !void {
        if (self.hasIncompletePending()) return error.PendingOperationsExist;
        if (self.pending.items.len != 0) self.clearAcked();

        var live = try self.collectLiveEntries(allocator);
        defer {
            for (live.items) |item| allocator.free(item.key);
            live.deinit(allocator);
        }

        const cwd = std.fs.cwd();

        const wal_target_path = try allocWalSegmentPath(allocator, self.id, 0);
        defer allocator.free(wal_target_path);

        const blob_target_path = try allocBlobSegmentPath(allocator, self.id, 0);
        defer allocator.free(blob_target_path);

        const wal_tmp_path = try std.fmt.allocPrint(allocator, "{s}.compact.tmp", .{wal_target_path});
        defer allocator.free(wal_tmp_path);

        const blob_tmp_path = try std.fmt.allocPrint(allocator, "{s}.compact.tmp", .{blob_target_path});
        defer allocator.free(blob_tmp_path);

        cwd.deleteFile(wal_tmp_path) catch {};
        cwd.deleteFile(blob_tmp_path) catch {};

        var wal_tmp = try cwd.createFile(wal_tmp_path, .{ .read = true, .truncate = true });
        var blob_tmp = try cwd.createFile(blob_tmp_path, .{ .read = true, .truncate = true });

        var new_wal_end: u64 = 0;
        var new_blob_end: u64 = 0;
        var max_seqno: u64 = 0;

        errdefer {
            wal_tmp.close();
            blob_tmp.close();
            cwd.deleteFile(wal_tmp_path) catch {};
            cwd.deleteFile(blob_tmp_path) catch {};
        }

        for (live.items) |item| {
            const seqno = switch (item.value) {
                .@"inline" => |x| x.seqno,
                .blob => |x| x.seqno,
                .tombstone => unreachable,
            };
            if (seqno > max_seqno) max_seqno = seqno;

            switch (item.value) {
                .@"inline" => |x| {
                    const value = try self.readInlineValueAt(allocator, x.wal);
                    defer allocator.free(value);

                    const wal_len = record.inlineWalSize(item.key.len, value.len);
                    const header = record.RecordHeader{
                        .magic = record.RecordMagic,
                        .total_len = wal_len,
                        .checksum = 0,
                        .seqno = seqno,
                        .key_len = @intCast(item.key.len),
                        .value_len = @intCast(value.len),
                        .record_type = @intFromEnum(types.RecordType.put_inline),
                        .durability = @intFromEnum(types.Durability.strict),
                        .flags = 0,
                    };

                    const rec = try record.encodeInline(allocator, header, item.key, value);
                    defer allocator.free(rec);

                    try wal_tmp.pwriteAll(rec, new_wal_end);
                    new_wal_end += rec.len;
                },
                .blob => |x| {
                    var src_blob_file_opt: ?std.fs.File = null;
                    defer if (src_blob_file_opt) |*f| f.close();

                    const src_blob_file = if (x.blob.segment_id == self.blob_active.id)
                        self.blob.file
                    else blk: {
                        src_blob_file_opt = try self.openBlobSegmentForRead(@intCast(x.blob.segment_id));
                        break :blk src_blob_file_opt.?;
                    };

                    const new_blob_ref = try blob_mod.copyLiveBlob(
                        allocator,
                        src_blob_file,
                        blob_tmp,
                        0,
                        &new_blob_end,
                        x.blob,
                    );

                    const wal_len = record.blobWalSize(item.key.len);
                    const header = record.RecordHeader{
                        .magic = record.RecordMagic,
                        .total_len = wal_len,
                        .checksum = 0,
                        .seqno = seqno,
                        .key_len = @intCast(item.key.len),
                        .value_len = new_blob_ref.value_len,
                        .record_type = @intFromEnum(types.RecordType.put_blob),
                        .durability = @intFromEnum(types.Durability.strict),
                        .flags = 0,
                    };

                    const rec = try record.encodeBlobRef(
                        allocator,
                        header,
                        item.key,
                        new_blob_ref,
                    );
                    defer allocator.free(rec);

                    try wal_tmp.pwriteAll(rec, new_wal_end);
                    new_wal_end += rec.len;
                },
                .tombstone => unreachable,
            }
        }

        try blob_tmp.sync();
        try wal_tmp.sync();

        wal_tmp.close();
        blob_tmp.close();

        self.wal.close(self.allocator);
        self.blob.close(self.allocator);

        try self.deleteAllSegments();

        try cwd.rename(wal_tmp_path, wal_target_path);
        try cwd.rename(blob_tmp_path, blob_target_path);

        self.wal = try wal_mod.Wal.openShardSegment(self.allocator, self.id, 0);
        self.blob = try blob_mod.Blob.openShardSegment(self.allocator, self.id, 0);

        self.wal_active.id = 0;
        self.blob_active.id = 0;

        try self.wal.file.setEndPos(new_wal_end);
        try self.blob.file.setEndPos(new_blob_end);

        self.wal_active.tail.reserved = new_wal_end;
        self.wal_active.tail.written = new_wal_end;
        self.wal_active.tail.durable = new_wal_end;

        self.blob_active.tail.reserved = new_blob_end;
        self.blob_active.tail.written = new_blob_end;
        self.blob_active.tail.durable = new_blob_end;

        self.clearIndexAndFreeKeys(allocator);

        const replay = try recovery.replayShardSegments(
            allocator,
            self.id,
            &self.index,
        );

        self.wal_active.id = replay.wal_segment_id;
        self.wal_active.tail.reserved = replay.wal_end;
        self.wal_active.tail.written = replay.wal_end;
        self.wal_active.tail.durable = replay.wal_end;

        self.blob_active.id = replay.blob_segment_id;
        self.blob_active.tail.reserved = replay.blob_end;
        self.blob_active.tail.written = replay.blob_end;
        self.blob_active.tail.durable = replay.blob_end;

        if (replay.next_seqno > self.next_seqno) {
            self.next_seqno = replay.next_seqno;
        } else {
            self.next_seqno = max_seqno + 1;
        }

        try self.rebuildStateIndexFromWal(allocator);

        self.dirty = false;
        self.dirty_since_ns = 0;
        self.strongest_dirty_durability = null;
        self.dirty_bytes = 0;
        self.dirty_ops = 0;
    }

    pub fn reserveSeqno(self: *Shard) u64 {
        const seq = self.next_seqno;
        self.next_seqno += 1;
        return seq;
    }

    pub fn enqueuePut(
        self: *Shard,
        durability: types.Durability,
        key: []const u8,
        value: []const u8,
    ) !u64 {
        const owned_key = try self.allocator.dupe(u8, key);
        errdefer self.allocator.free(owned_key);

        const owned_value = try self.allocator.dupe(u8, value);
        errdefer self.allocator.free(owned_value);

        var op = types.PendingOp{
            .seqno = self.reserveSeqno(),
            .durability = durability,
            .record_type = .put_inline,
            .key = owned_key,
            .value = owned_value,
        };

        try self.layoutOp(&op);
        try self.pending.append(self.allocator, op);
        return op.seqno;
    }

    pub fn enqueueDelete(
        self: *Shard,
        durability: types.Durability,
        key: []const u8,
    ) !u64 {
        const owned_key = try self.allocator.dupe(u8, key);
        errdefer self.allocator.free(owned_key);

        var op = types.PendingOp{
            .seqno = self.reserveSeqno(),
            .durability = durability,
            .record_type = .tombstone,
            .key = owned_key,
            .value = &.{},
        };

        try self.layoutOp(&op);
        try self.pending.append(self.allocator, op);
        return op.seqno;
    }

    fn layoutOp(self: *Shard, op: *types.PendingOp) !void {
        const decision = self.decideLayout(op.*);

        switch (decision) {
            .@"inline" => |d| {
                try self.ensureWalSpace(d.wal_len);

                op.inline_value = true;
                op.record_type = .put_inline;
                op.wal_ref = .{
                    .segment_id = self.wal_active.id,
                    .offset = self.wal_active.tail.reserved,
                    .len = d.wal_len,
                };

                self.wal_active.tail.reserved += d.wal_len;
            },
            .blob => |d| {
                try self.ensureBlobSpace(d.blob_len);
                try self.ensureWalSpace(d.wal_len);

                op.inline_value = false;
                op.record_type = .put_blob;

                op.blob_ref = .{
                    .segment_id = self.blob_active.id,
                    .offset = self.blob_active.tail.reserved,
                    .total_len = d.blob_len,
                    .value_len = @intCast(op.value.len),
                    .checksum = 0,
                };

                op.wal_ref = .{
                    .segment_id = self.wal_active.id,
                    .offset = self.wal_active.tail.reserved,
                    .len = d.wal_len,
                };

                self.blob_active.tail.reserved += d.blob_len;
                self.wal_active.tail.reserved += d.wal_len;
            },
            .tombstone => |d| {
                try self.ensureWalSpace(d.wal_len);

                op.record_type = .tombstone;
                op.wal_ref = .{
                    .segment_id = self.wal_active.id,
                    .offset = self.wal_active.tail.reserved,
                    .len = d.wal_len,
                };

                self.wal_active.tail.reserved += d.wal_len;
            },
        }

        op.state = .layout_done;
    }

    fn decideLayout(self: *Shard, op: types.PendingOp) types.LayoutDecision {
        return switch (op.record_type) {
            .tombstone => .{
                .tombstone = .{
                    .wal_len = record.tombstoneWalSize(op.key.len),
                },
            },
            .put_inline, .put_blob => blk: {
                if (op.value.len <= self.config.inline_max) {
                    break :blk .{
                        .@"inline" = .{
                            .wal_len = record.inlineWalSize(op.key.len, op.value.len),
                        },
                    };
                }

                break :blk .{
                    .blob = .{
                        .blob_len = record.blobRecordSize(op.value.len),
                        .wal_len = record.blobWalSize(op.key.len),
                    },
                };
            },
        };
    }

    fn ensureWalSpace(self: *Shard, len: u32) !void {
        if (self.wal_active.remaining() < len) {
            try self.rotateWal();
        }
    }

    fn ensureBlobSpace(self: *Shard, len: u32) !void {
        if (self.blob_active.remaining() < len) {
            try self.rotateBlob();
        }
    }

    fn rotateWal(self: *Shard) !void {
        try self.wal.syncData();
        self.wal.close(self.allocator);

        self.wal_active.id += 1;
        self.wal_active.tail = .{};

        self.wal = try wal_mod.Wal.openShardSegment(
            self.allocator,
            self.id,
            self.wal_active.id,
        );
    }

    fn rotateBlob(self: *Shard) !void {
        try self.blob.syncData();
        self.blob.close(self.allocator);

        self.blob_active.id += 1;
        self.blob_active.tail = .{};

        self.blob = try blob_mod.Blob.openShardSegment(
            self.allocator,
            self.id,
            self.blob_active.id,
        );
    }

    pub fn commitPendingAsWritten(
        self: *Shard,
        now_ns: u64,
        backend: *io_backend.Backend,
    ) !void {
        var ops = std.ArrayListUnmanaged(io_backend.WriteOp){};
        defer ops.deinit(self.allocator);

        var buffers = std.ArrayListUnmanaged([]u8){};
        defer {
            for (buffers.items) |buf| self.allocator.free(buf);
            buffers.deinit(self.allocator);
        }

        var wal_files = std.ArrayListUnmanaged(OpenedSegmentFile){};
        defer {
            for (wal_files.items) |*item| item.file.close();
            wal_files.deinit(self.allocator);
        }

        var blob_files = std.ArrayListUnmanaged(OpenedSegmentFile){};
        defer {
            for (blob_files.items) |*item| item.file.close();
            blob_files.deinit(self.allocator);
        }

        for (self.pending.items) |op| {
            if (op.state != .layout_done) continue;

            switch (op.record_type) {
                .put_inline => {
                    const wal_ref = op.wal_ref.?;
                    const wal_file = try self.walFileForSegment(&wal_files, @intCast(wal_ref.segment_id));

                    const header = record.RecordHeader{
                        .magic = record.RecordMagic,
                        .total_len = wal_ref.len,
                        .checksum = 0,
                        .seqno = op.seqno,
                        .key_len = @intCast(op.key.len),
                        .value_len = @intCast(op.value.len),
                        .record_type = @intFromEnum(op.record_type),
                        .durability = @intFromEnum(op.durability),
                        .flags = 0,
                    };

                    const buf = try record.encodeInline(
                        self.allocator,
                        header,
                        op.key,
                        op.value,
                    );
                    try buffers.append(self.allocator, buf);
                    try ops.append(self.allocator, .{
                        .file = wal_file,
                        .offset = wal_ref.offset,
                        .data = buf,
                    });
                },
                .put_blob => {
                    const wal_ref = op.wal_ref.?;
                    var blob_ref = op.blob_ref.?;
                    blob_ref.checksum = record.crc32WithZeroTrailer(op.value);

                    const wal_file = try self.walFileForSegment(&wal_files, @intCast(wal_ref.segment_id));
                    const blob_file = try self.blobFileForSegment(&blob_files, @intCast(blob_ref.segment_id));

                    const blob_header = record.BlobHeader{
                        .magic = record.BlobMagic,
                        .total_len = blob_ref.total_len,
                        .checksum = blob_ref.checksum,
                        .value_len = @intCast(op.value.len),
                        .flags = 0,
                    };

                    const blob_buf = try record.encodeBlobPayload(
                        self.allocator,
                        blob_header,
                        op.value,
                    );
                    try buffers.append(self.allocator, blob_buf);
                    try ops.append(self.allocator, .{
                        .file = blob_file,
                        .offset = blob_ref.offset,
                        .data = blob_buf,
                    });

                    const wal_header = record.RecordHeader{
                        .magic = record.RecordMagic,
                        .total_len = wal_ref.len,
                        .checksum = 0,
                        .seqno = op.seqno,
                        .key_len = @intCast(op.key.len),
                        .value_len = @intCast(op.value.len),
                        .record_type = @intFromEnum(op.record_type),
                        .durability = @intFromEnum(op.durability),
                        .flags = 0,
                    };

                    const wal_buf = try record.encodeBlobRef(
                        self.allocator,
                        wal_header,
                        op.key,
                        blob_ref,
                    );
                    try buffers.append(self.allocator, wal_buf);
                    try ops.append(self.allocator, .{
                        .file = wal_file,
                        .offset = wal_ref.offset,
                        .data = wal_buf,
                    });
                },
                .tombstone => {
                    const wal_ref = op.wal_ref.?;
                    const wal_file = try self.walFileForSegment(&wal_files, @intCast(wal_ref.segment_id));

                    const header = record.RecordHeader{
                        .magic = record.RecordMagic,
                        .total_len = wal_ref.len,
                        .checksum = 0,
                        .seqno = op.seqno,
                        .key_len = @intCast(op.key.len),
                        .value_len = 0,
                        .record_type = @intFromEnum(op.record_type),
                        .durability = @intFromEnum(op.durability),
                        .flags = 0,
                    };

                    const buf = try record.encodeTombstone(
                        self.allocator,
                        header,
                        op.key,
                    );
                    try buffers.append(self.allocator, buf);
                    try ops.append(self.allocator, .{
                        .file = wal_file,
                        .offset = wal_ref.offset,
                        .data = buf,
                    });
                },
            }
        }

        try backend.writeBatch(ops.items);

        for (self.pending.items) |*op| {
            if (op.state != .layout_done) continue;

            switch (op.record_type) {
                .put_inline => {
                    const wal_ref = op.wal_ref.?;
                    if (wal_ref.segment_id == self.wal_active.id) {
                        self.wal_active.tail.written =
                            @max(self.wal_active.tail.written, wal_ref.offset + wal_ref.len);
                    }

                    const gop = try self.index.getOrPut(self.allocator, op.key);
                    if (!gop.found_existing) {
                        gop.key_ptr.* = try self.allocator.dupe(u8, op.key);
                    }

                    gop.value_ptr.* = .{
                        .@"inline" = .{
                            .wal = wal_ref,
                            .seqno = op.seqno,
                        },
                    };

                    try self.upsertStateIndex(self.allocator, op.key, .{
                        .wal_segment_id = wal_ref.segment_id,
                        .wal_offset = wal_ref.offset,
                        .seqno = op.seqno,
                        .is_tombstone = false,
                    });
                },
                .put_blob => {
                    const wal_ref = op.wal_ref.?;
                    var blob_ref = op.blob_ref.?;
                    blob_ref.checksum = record.crc32WithZeroTrailer(op.value);
                    op.blob_ref = blob_ref;

                    if (blob_ref.segment_id == self.blob_active.id) {
                        self.blob_active.tail.written =
                            @max(self.blob_active.tail.written, blob_ref.offset + blob_ref.total_len);
                    }
                    if (wal_ref.segment_id == self.wal_active.id) {
                        self.wal_active.tail.written =
                            @max(self.wal_active.tail.written, wal_ref.offset + wal_ref.len);
                    }

                    const gop = try self.index.getOrPut(self.allocator, op.key);
                    if (!gop.found_existing) {
                        gop.key_ptr.* = try self.allocator.dupe(u8, op.key);
                    }

                    gop.value_ptr.* = .{
                        .blob = .{
                            .blob = blob_ref,
                            .seqno = op.seqno,
                        },
                    };

                    try self.upsertStateIndex(self.allocator, op.key, .{
                        .wal_segment_id = wal_ref.segment_id,
                        .wal_offset = wal_ref.offset,
                        .seqno = op.seqno,
                        .is_tombstone = false,
                    });
                },
                .tombstone => {
                    const wal_ref = op.wal_ref.?;
                    if (wal_ref.segment_id == self.wal_active.id) {
                        self.wal_active.tail.written =
                            @max(self.wal_active.tail.written, wal_ref.offset + wal_ref.len);
                    }

                    const gop = try self.index.getOrPut(self.allocator, op.key);
                    if (!gop.found_existing) {
                        gop.key_ptr.* = try self.allocator.dupe(u8, op.key);
                    }

                    gop.value_ptr.* = .{
                        .tombstone = .{
                            .seqno = op.seqno,
                        },
                    };
                    try self.upsertStateIndex(self.allocator, op.key, .{
                        .wal_segment_id = wal_ref.segment_id,
                        .wal_offset = wal_ref.offset,
                        .seqno = op.seqno,
                        .is_tombstone = true,
                    });
                },
            }

            op.state = .wal_completed;

            self.frontier.visible = .{
                .wal_segment_id = self.wal_active.id,
                .wal_offset = self.wal_active.tail.written,
            };

            var bytes: u64 = 0;
            if (op.wal_ref) |wal_ref| bytes += wal_ref.len;
            if (op.blob_ref) |blob_ref| bytes += blob_ref.total_len;
            self.noteDirty(now_ns, op.durability, bytes);
        }
    }

    pub fn strongestPendingDurability(self: *const Shard) ?types.Durability {
        var strongest: ?types.Durability = null;

        for (self.pending.items) |op| {
            if (op.state == .acked) continue;

            if (strongest == null) {
                strongest = op.durability;
                continue;
            }

            if (@intFromEnum(op.durability) > @intFromEnum(strongest.?)) {
                strongest = op.durability;
            }
        }

        return strongest;
    }

    fn noteDirty(
        self: *Shard,
        now_ns: u64,
        durability: types.Durability,
        bytes: u64,
    ) void {
        if (!self.dirty) {
            self.dirty = true;
            self.dirty_since_ns = now_ns;
            self.strongest_dirty_durability = durability;
        } else if (self.strongest_dirty_durability == null or
            @intFromEnum(durability) > @intFromEnum(self.strongest_dirty_durability.?))
        {
            self.strongest_dirty_durability = durability;
        }

        self.dirty_bytes += bytes;
        self.dirty_ops += 1;
    }

    pub fn syncData(self: *Shard, now_ns: u64) !void {
        if (!self.dirty) return;

        try self.blob.syncData();
        try self.wal.syncData();

        self.markDurable();
        self.dirty = false;
        self.dirty_since_ns = 0;
        self.last_sync_ns = now_ns;
        self.strongest_dirty_durability = null;
        self.dirty_bytes = 0;
        self.dirty_ops = 0;
    }

    pub fn markDurable(self: *Shard) void {
        self.wal_active.tail.durable = self.wal_active.tail.written;
        self.blob_active.tail.durable = self.blob_active.tail.written;
        self.frontier.durable = .{
            .wal_segment_id = self.wal_active.id,
            .wal_offset = self.wal_active.tail.durable,
        };

        for (self.pending.items) |*op| {
            if (op.state == .wal_completed) op.state = .durable;
        }
    }

    pub fn clearAcked(self: *Shard) void {
        for (self.pending.items) |*op| {
            switch (op.state) {
                .durable, .acked => {
                    op.state = .acked;
                    self.allocator.free(op.key);
                    self.allocator.free(op.value);
                },
                .queued, .layout_done, .wal_completed => return,
            }
        }
        self.pending.clearRetainingCapacity();
    }

    fn cursorLessThan(a: types.ShardCursor, b: types.ShardCursor) bool {
        if (a.wal_segment_id < b.wal_segment_id) return true;
        if (a.wal_segment_id > b.wal_segment_id) return false;
        return a.wal_offset < b.wal_offset;
    }

    fn cursorLessThanOrEqual(a: types.ShardCursor, b: types.ShardCursor) bool {
        return !cursorLessThan(b, a);
    }

    pub fn readLimit(self: *const Shard, consistency: types.ReadConsistency) types.ShardCursor {
        return switch (consistency) {
            .visible => self.frontier.visible,
            .durable => self.frontier.durable,
        };
    }

    pub fn get(self: *Shard, key: []const u8) ?types.ValueRef {
        return self.index.get(key);
    }

    pub fn readValue(self: *Shard, allocator: std.mem.Allocator, key: []const u8) !?[]u8 {
        const entry = self.state_index.get(key) orelse return null;
        return try self.readValueAtStateIndexEntry(allocator, entry);
    }

    pub fn getAt(
        self: *Shard,
        allocator: std.mem.Allocator,
        cursor: types.Cursor,
        key: []const u8,
    ) !?[]u8 {
        if (cursor.shard_id != self.id) return error.InvalidCursorShard;

        const visible = self.readLimit(.visible);
        const target: types.ShardCursor = .{
            .wal_segment_id = cursor.wal_segment_id,
            .wal_offset = cursor.wal_offset,
        };

        if (cursorLessThan(visible, target)) return error.CursorBeyondVisibleFrontier;

        var scan_cursor = types.Cursor{
            .shard_id = self.id,
            .wal_segment_id = 0,
            .wal_offset = 0,
        };

        var last_value: ?[]u8 = null;
        var saw_tombstone = false;
        errdefer if (last_value) |buf| allocator.free(buf);

        while (true) {
            var items = std.ArrayListUnmanaged(types.StreamItem){};
            defer {
                for (items.items) |item| freeStreamItem(allocator, item);
                items.deinit(allocator);
            }

            const next_cursor = try self.readFromAppendUntil(
                allocator,
                scan_cursor,
                256,
                target,
                &items,
            );

            for (items.items) |item| {
                if (!std.mem.eql(u8, item.key, key)) continue;

                if (last_value) |buf| {
                    allocator.free(buf);
                    last_value = null;
                }

                switch (item.value) {
                    .@"inline" => |v| {
                        last_value = try allocator.dupe(u8, v);
                        saw_tombstone = false;
                    },
                    .blob => |v| {
                        last_value = try allocator.dupe(u8, v);
                        saw_tombstone = false;
                    },
                    .tombstone => {
                        saw_tombstone = true;
                    },
                }
            }

            if (items.items.len == 0) break;
            scan_cursor = next_cursor;
        }

        if (saw_tombstone) return null;
        return last_value;
    }

    pub fn readFrom(
        self: *Shard,
        allocator: std.mem.Allocator,
        start: types.Cursor,
        limit: usize,
    ) !types.ReadFromResult {
        return self.readFromConsistent(allocator, start, limit, .visible);
    }

    pub fn getStateIndexEntry(
        self: *Shard,
        key: []const u8,
    ) ?StateIndexEntry {
        return self.state_index.get(key);
    }

    pub fn readFromConsistent(
        self: *Shard,
        allocator: std.mem.Allocator,
        start: types.Cursor,
        limit: usize,
        consistency: types.ReadConsistency,
    ) !types.ReadFromResult {
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

        const next_cursor = try self.readFromAppendUntil(allocator, start, limit, self.readLimit(consistency), &temp_items);

        var arena_items = std.ArrayListUnmanaged(types.StreamItem){};
        errdefer {
            for (arena_items.items) |item| {
                freeStreamItem(result_allocator, item);
            }
            arena_items.deinit(result_allocator);
        }

        try arena_items.ensureTotalCapacity(result_allocator, temp_items.items.len);

        for (temp_items.items) |item| {
            const key = try result_allocator.dupe(u8, item.key);
            errdefer result_allocator.free(key);

            const value: types.StreamValue = switch (item.value) {
                .@"inline" => |v| .{ .@"inline" = try result_allocator.dupe(u8, v) },
                .blob => |v| .{ .blob = try result_allocator.dupe(u8, v) },
                .tombstone => .tombstone,
            };
            errdefer switch (value) {
                .@"inline" => |v| result_allocator.free(v),
                .blob => |v| result_allocator.free(v),
                .tombstone => {},
            };

            arena_items.appendAssumeCapacity(.{
                .seqno = item.seqno,
                .key = key,
                .value = value,
                .durability = item.durability,
                .record_type = item.record_type,
                .cursor = item.cursor,
            });
        }

        for (temp_items.items) |item| {
            freeStreamItem(allocator, item);
        }
        temp_items.deinit(allocator);

        return .{
            .items = try arena_items.toOwnedSlice(result_allocator),
            .next_cursor = next_cursor,
            .arena = result_arena,
        };
    }

    pub fn readFromAppend(
        self: *Shard,
        allocator: std.mem.Allocator,
        cursor: types.Cursor,
        limit: usize,
        out: *std.ArrayListUnmanaged(types.StreamItem),
    ) !types.Cursor {
        return self.readFromAppendUntil(allocator, cursor, limit, self.frontier.visible, out);
    }

    pub fn readFromAppendUntil(
        self: *Shard,
        allocator: std.mem.Allocator,
        cursor: types.Cursor,
        limit: usize,
        max_cursor: types.ShardCursor,
        out: *std.ArrayListUnmanaged(types.StreamItem),
    ) !types.Cursor {
        const start_len = out.items.len;
        errdefer {
            var i = start_len;
            while (i < out.items.len) : (i += 1) {
                freeStreamItem(allocator, out.items[i]);
            }
            out.shrinkRetainingCapacity(start_len);
        }

        var seg_id = cursor.wal_segment_id;
        var offset = cursor.wal_offset;

        if (cursorLessThanOrEqual(max_cursor, .{ .wal_segment_id = seg_id, .wal_offset = offset })) {
            return .{
                .shard_id = self.id,
                .wal_segment_id = seg_id,
                .wal_offset = offset,
            };
        }

        var scratch = std.ArrayListUnmanaged(u8){};
        defer scratch.deinit(allocator);

        while (out.items.len - start_len < limit) {
            var file = (try self.openWalSegmentIfExistsForCursor(seg_id)) orelse break;
            defer file.close();

            while (out.items.len - start_len < limit) {
                var header: record.RecordHeader = undefined;
                const n = try file.pread(std.mem.asBytes(&header), offset);

                if (n == 0) break;
                if (n != @sizeOf(record.RecordHeader)) return error.TruncatedHeader;
                if (header.magic != record.RecordMagic) return error.InvalidMagic;

                const total_len: usize = @intCast(header.total_len);
                if (total_len < @sizeOf(record.RecordHeader) + 4) {
                    return error.InvalidRecord;
                }

                try scratch.resize(allocator, total_len);
                const full_buf = scratch.items[0..total_len];

                const read_n = try file.pread(full_buf, offset);
                if (read_n != total_len) return error.TruncatedValue;

                const payload = full_buf[@sizeOf(record.RecordHeader)..];
                if (record.crc32(payload) != header.checksum) {
                    return error.ChecksumMismatch;
                }

                const rec_type: types.RecordType = @enumFromInt(header.record_type);
                const durability: types.Durability = @enumFromInt(header.durability);
                const key_len: usize = @intCast(header.key_len);
                const value_len: usize = @intCast(header.value_len);

                const key_start = @sizeOf(record.RecordHeader);
                const key_end = key_start + key_len;
                if (key_end > full_buf.len) return error.InvalidRecord;

                const key_src = full_buf[key_start..key_end];

                const next_offset = offset + header.total_len;
                const item_next_cursor: types.ShardCursor = .{
                    .wal_segment_id = seg_id,
                    .wal_offset = next_offset,
                };
                if (cursorLessThan(max_cursor, item_next_cursor)) {
                    return .{
                        .shard_id = self.id,
                        .wal_segment_id = seg_id,
                        .wal_offset = offset,
                    };
                }

                var owned_key: ?[]u8 = null;
                errdefer if (owned_key) |k| allocator.free(k);

                var stream_value: types.StreamValue = undefined;
                var stream_value_ready = false;
                errdefer if (stream_value_ready) switch (stream_value) {
                    .@"inline" => |v| allocator.free(v),
                    .blob => |v| allocator.free(v),
                    .tombstone => {},
                };

                switch (rec_type) {
                    .put_inline => {
                        const value_start = key_end;
                        const value_end = value_start + value_len;
                        if (value_end + 4 > full_buf.len) return error.InvalidRecord;

                        owned_key = try allocator.dupe(u8, key_src);

                        const owned_value = try allocator.dupe(u8, full_buf[value_start..value_end]);
                        stream_value = .{ .@"inline" = owned_value };
                        stream_value_ready = true;
                    },
                    .put_blob => {
                        const blob_ref_start = key_end;
                        const blob_ref_end = blob_ref_start + @sizeOf(types.BlobRef);
                        if (blob_ref_end + 4 > full_buf.len) return error.InvalidRecord;

                        owned_key = try allocator.dupe(u8, key_src);

                        const blob_ref = std.mem.bytesToValue(
                            types.BlobRef,
                            full_buf[blob_ref_start..blob_ref_end],
                        );

                        const value = try self.readBlobValue(allocator, blob_ref);
                        stream_value = .{ .blob = value };
                        stream_value_ready = true;
                    },
                    .tombstone => {
                        owned_key = try allocator.dupe(u8, key_src);
                        stream_value = .tombstone;
                        stream_value_ready = true;
                    },
                }

                try out.append(allocator, .{
                    .seqno = header.seqno,
                    .key = owned_key.?,
                    .value = stream_value,
                    .durability = durability,
                    .record_type = rec_type,
                    .cursor = .{
                        .shard_id = self.id,
                        .wal_segment_id = seg_id,
                        .wal_offset = next_offset,
                    },
                });

                owned_key = null;
                stream_value_ready = false;

                offset = next_offset;
            }

            if (cursorLessThanOrEqual(max_cursor, .{ .wal_segment_id = seg_id, .wal_offset = offset })) break;

            if (out.items.len - start_len >= limit) break;

            seg_id += 1;
            offset = 0;
        }

        return .{
            .shard_id = self.id,
            .wal_segment_id = seg_id,
            .wal_offset = offset,
        };
    }

    fn freeStreamItem(allocator: std.mem.Allocator, item: types.StreamItem) void {
        allocator.free(item.key);
        switch (item.value) {
            .@"inline" => |v| allocator.free(v),
            .blob => |v| allocator.free(v),
            .tombstone => {},
        }
    }

    fn allocWalSegmentPathForCursor(
        allocator: std.mem.Allocator,
        shard_id: u16,
        segment_id: u32,
    ) ![]u8 {
        if (segment_id == 0) {
            return try std.fmt.allocPrint(allocator, "wal-{d:0>3}.log", .{shard_id});
        }
        return try std.fmt.allocPrint(
            allocator,
            "wal-{d:0>3}-{d:0>3}.log",
            .{ shard_id, segment_id },
        );
    }

    fn openWalSegmentIfExistsForCursor(
        self: *Shard,
        segment_id: u32,
    ) !?std.fs.File {
        const path = try allocWalSegmentPathForCursor(self.allocator, self.id, segment_id);
        defer self.allocator.free(path);

        return std.fs.cwd().openFile(path, .{ .mode = .read_only }) catch |err| switch (err) {
            error.FileNotFound => null,
            else => return err,
        };
    }

    fn readInlineValue(
        self: *Shard,
        allocator: std.mem.Allocator,
        wal_ref: types.SegmentRef,
    ) ![]u8 {
        return try self.readInlineValueAt(allocator, wal_ref);
    }

    pub fn readValueAtStateIndexEntry(
        self: *Shard,
        allocator: std.mem.Allocator,
        entry: StateIndexEntry,
    ) !?[]u8 {
        if (entry.is_tombstone) return null;

        var maybe_file: ?std.fs.File = null;
        defer if (maybe_file) |*f| f.close();

        const file = if (entry.wal_segment_id == self.wal_active.id)
            self.wal.file
        else blk: {
            maybe_file = try self.openWalSegmentForRead(@intCast(entry.wal_segment_id));
            break :blk maybe_file.?;
        };

        var header: record.RecordHeader = undefined;
        const header_bytes = try file.pread(std.mem.asBytes(&header), entry.wal_offset);
        if (header_bytes != @sizeOf(record.RecordHeader)) return error.TruncatedHeader;
        if (header.magic != record.RecordMagic) return error.InvalidMagic;

        const total_len: usize = @intCast(header.total_len);
        if (total_len < @sizeOf(record.RecordHeader) + 4) return error.InvalidRecord;

        var buf = try allocator.alloc(u8, total_len);
        defer allocator.free(buf);

        const n = try file.pread(buf, entry.wal_offset);
        if (n != total_len) return error.TruncatedValue;

        const payload = buf[@sizeOf(record.RecordHeader)..];
        if (record.crc32(payload) != header.checksum) return error.ChecksumMismatch;

        const rec_type: types.RecordType = @enumFromInt(header.record_type);
        const key_len: usize = @intCast(header.key_len);
        const value_len: usize = @intCast(header.value_len);

        const key_start = @sizeOf(record.RecordHeader);
        const key_end = key_start + key_len;
        if (key_end > buf.len) return error.InvalidRecord;

        switch (rec_type) {
            .put_inline => {
                const value_start = key_end;
                const value_end = value_start + value_len;
                if (value_end + 4 > buf.len) return error.InvalidRecord;

                return try allocator.dupe(u8, buf[value_start..value_end]);
            },
            .put_blob => {
                const blob_ref_start = key_end;
                const blob_ref_end = blob_ref_start + @sizeOf(types.BlobRef);
                if (blob_ref_end + 4 > buf.len) return error.InvalidRecord;

                const blob_ref = std.mem.bytesToValue(
                    types.BlobRef,
                    buf[blob_ref_start..blob_ref_end],
                );

                return try self.readBlobValue(allocator, blob_ref);
            },
            .tombstone => return null,
        }
    }

    fn readBlobValue(
        self: *Shard,
        allocator: std.mem.Allocator,
        blob_ref: types.BlobRef,
    ) ![]u8 {
        var maybe_file: ?std.fs.File = null;
        defer if (maybe_file) |*f| f.close();

        const file = if (blob_ref.segment_id == self.blob_active.id)
            self.blob.file
        else blk: {
            maybe_file = try self.openBlobSegmentForRead(@intCast(blob_ref.segment_id));
            break :blk maybe_file.?;
        };

        return try blob_mod.readBlobValue(allocator, file, blob_ref);
    }

    pub fn freeReadFromResult(
        allocator: std.mem.Allocator,
        result: types.ReadFromResult,
    ) void {
        if (result.arena) |arena| {
            var owned_arena = arena;
            owned_arena.deinit();
        } else {
            for (result.items) |item| {
                allocator.free(item.key);
                switch (item.value) {
                    .@"inline" => |v| allocator.free(v),
                    .blob => |v| allocator.free(v),
                    .tombstone => {},
                }
            }
            allocator.free(result.items);
        }
    }
};

test "state index updates when pending ops become visible" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();

    const allocator = gpa.allocator();

    var shard = try Shard.init(allocator, 0, .{
        .shard_count = 1,
        .inline_max = 16,
    });
    defer shard.deinit();

    _ = try shard.enqueuePut(.strict, "user:1", "hello");

    var backend = try io_backend.Backend.init(.pwrite, 128);
    defer backend.deinit();

    try shard.commitPendingAsWritten(0, &backend);

    const found = shard.state_index.get("user:1");
    try std.testing.expect(found != null);
    try std.testing.expect(!found.?.is_tombstone);
}
