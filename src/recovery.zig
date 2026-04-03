const std = @import("std");
const record = @import("record.zig");
const types = @import("types.zig");

pub const ReplayResult = struct {
    wal_segment_id: u32 = 0,
    wal_end: u64 = 0,
    blob_segment_id: u32 = 0,
    blob_end: u64 = 0,
    next_seqno: u64 = 1,
};

const MaxReplayRecordSize: u32 = 128 * 1024 * 1024;

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

fn openWalSegmentIfExists(
    allocator: std.mem.Allocator,
    shard_id: u16,
    segment_id: u32,
) !?std.fs.File {
    const path = try allocWalSegmentPath(allocator, shard_id, segment_id);
    defer allocator.free(path);

    return std.fs.cwd().openFile(path, .{ .mode = .read_only }) catch |err| switch (err) {
        error.FileNotFound => null,
        else => return err,
    };
}

fn openBlobSegmentForRead(
    allocator: std.mem.Allocator,
    shard_id: u16,
    segment_id: u32,
) !std.fs.File {
    const path = try allocBlobSegmentPath(allocator, shard_id, segment_id);
    defer allocator.free(path);

    return try std.fs.cwd().openFile(path, .{ .mode = .read_only });
}

const BlobSegmentCache = struct {
    allocator: std.mem.Allocator,
    shard_id: u16,
    segment_id: ?u32 = null,
    file: ?std.fs.File = null,

    pub fn init(allocator: std.mem.Allocator, shard_id: u16) BlobSegmentCache {
        return .{
            .allocator = allocator,
            .shard_id = shard_id,
        };
    }

    pub fn deinit(self: *BlobSegmentCache) void {
        if (self.file) |f| {
            f.close();
            self.file = null;
        }
        self.segment_id = null;
    }

    pub fn getFile(self: *BlobSegmentCache, segment_id: u32) !*std.fs.File {
        if (self.segment_id) |current| {
            if (current == segment_id) {
                return &(self.file.?);
            }
            self.file.?.close();
            self.file = null;
            self.segment_id = null;
        }

        self.file = try openBlobSegmentForRead(self.allocator, self.shard_id, segment_id);
        self.segment_id = segment_id;
        return &(self.file.?);
    }
};

fn validateBlob(
    allocator: std.mem.Allocator,
    file: std.fs.File,
    blob_ref: types.BlobRef,
) !bool {
    var header: record.BlobHeader = undefined;

    const n = try file.pread(std.mem.asBytes(&header), blob_ref.offset);
    if (n != @sizeOf(record.BlobHeader)) return false;

    if (header.magic != record.BlobMagic) return false;
    if (header.total_len != blob_ref.total_len) return false;
    if (header.value_len != blob_ref.value_len) return false;
    if (header.checksum != blob_ref.checksum) return false;

    const payload_len: usize = @intCast(header.total_len - @sizeOf(record.BlobHeader));
    if (payload_len < 4) return false;
    if (header.value_len + 4 != payload_len) return false;

    var buf = try allocator.alloc(u8, payload_len);
    defer allocator.free(buf);

    const read_n = try file.pread(buf, blob_ref.offset + @sizeOf(record.BlobHeader));
    if (read_n != payload_len) return false;

    const computed = record.crc32(buf);
    if (computed != header.checksum) return false;

    const trailer = buf[header.value_len..];
    return std.mem.eql(u8, trailer, &[_]u8{ 0, 0, 0, 0 });
}

fn upsertReplayValue(
    allocator: std.mem.Allocator,
    index: *types.IndexMap,
    key_src: []const u8,
    value: types.ValueRef,
) !void {
    if (index.getEntry(key_src)) |entry| {
        entry.value_ptr.* = value;
        return;
    }

    const key_buf = try allocator.dupe(u8, key_src);
    errdefer allocator.free(key_buf);

    try index.put(allocator, key_buf, value);
}

fn replayWalSegment(
    allocator: std.mem.Allocator,
    file: std.fs.File,
    shard_id: u16,
    wal_segment_id: u32,
    index: *types.IndexMap,
) !ReplayResult {
    var result = ReplayResult{
        .wal_segment_id = wal_segment_id,
    };
    var offset: u64 = 0;
    var blob_cache = BlobSegmentCache.init(allocator, shard_id);
    defer blob_cache.deinit();

    while (true) {
        var header: record.RecordHeader = undefined;

        const n = try file.pread(std.mem.asBytes(&header), offset);
        if (n == 0) break;
        if (n != @sizeOf(record.RecordHeader)) break;
        if (header.magic != record.RecordMagic) break;

        if (header.total_len < @sizeOf(record.RecordHeader) + 4) break;
        if (header.total_len > MaxReplayRecordSize) break;

        const total_len: usize = @intCast(header.total_len);

        var full_buf = try allocator.alloc(u8, total_len);
        defer allocator.free(full_buf);

        const read_full = try file.pread(full_buf, offset);
        if (read_full != total_len) break;

        const payload = full_buf[@sizeOf(record.RecordHeader)..];
        const computed_crc = record.crc32(payload);
        if (computed_crc != header.checksum) break;

        const rec_type: types.RecordType = @enumFromInt(header.record_type);
        const key_len: usize = @intCast(header.key_len);
        const value_len: usize = @intCast(header.value_len);

        const expected_total_len: usize = switch (rec_type) {
            .put_inline => @sizeOf(record.RecordHeader) + key_len + value_len + 4,
            .put_blob => @sizeOf(record.RecordHeader) + key_len + @sizeOf(types.BlobRef) + 4,
            .tombstone => @sizeOf(record.RecordHeader) + key_len + 4,
        };

        if (expected_total_len != total_len) break;

        const key_start = @sizeOf(record.RecordHeader);
        const key_end = key_start + key_len;
        if (key_end > full_buf.len) break;

        const key_src = full_buf[key_start..key_end];

        switch (rec_type) {
            .put_inline => {
                try upsertReplayValue(allocator, index, key_src, .{
                    .@"inline" = .{
                        .wal = .{
                            .segment_id = wal_segment_id,
                            .offset = offset,
                            .len = header.total_len,
                        },
                        .seqno = header.seqno,
                    },
                });
            },
            .put_blob => {
                const blob_ref_start = key_end;
                const blob_ref_end = blob_ref_start + @sizeOf(types.BlobRef);
                if (blob_ref_end > full_buf.len) break;

                const blob_ref = std.mem.bytesToValue(
                    types.BlobRef,
                    full_buf[blob_ref_start..blob_ref_end],
                );

                if (blob_ref.total_len < @sizeOf(record.BlobHeader)) break;
                if (blob_ref.total_len > MaxReplayRecordSize) break;

                const blob_seg_file = try blob_cache.getFile(@intCast(blob_ref.segment_id));
                const ok = try validateBlob(allocator, blob_seg_file.*, blob_ref);
                if (!ok) break;

                try upsertReplayValue(allocator, index, key_src, .{
                    .blob = .{
                        .blob = blob_ref,
                        .seqno = header.seqno,
                    },
                });

                const blob_end = blob_ref.offset + blob_ref.total_len;
                if (blob_ref.segment_id > result.blob_segment_id or
                    (blob_ref.segment_id == result.blob_segment_id and blob_end > result.blob_end))
                {
                    result.blob_segment_id = @intCast(blob_ref.segment_id);
                    result.blob_end = blob_end;
                }
            },
            .tombstone => {
                try upsertReplayValue(allocator, index, key_src, .{
                    .tombstone = .{
                        .seqno = header.seqno,
                    },
                });
            },
        }

        const rec_end = offset + header.total_len;
        result.wal_end = rec_end;

        if (header.seqno + 1 > result.next_seqno) {
            result.next_seqno = header.seqno + 1;
        }

        offset = rec_end;
    }

    return result;
}

pub fn replayShardSegments(
    allocator: std.mem.Allocator,
    shard_id: u16,
    index: *types.IndexMap,
) !ReplayResult {
    var result = ReplayResult{};
    var wal_segment_id: u32 = 0;

    while (true) {
        var wal_file = (try openWalSegmentIfExists(allocator, shard_id, wal_segment_id)) orelse break;
        defer wal_file.close();

        const seg_result = try replayWalSegment(
            allocator,
            wal_file,
            shard_id,
            wal_segment_id,
            index,
        );

        result.wal_segment_id = wal_segment_id;
        result.wal_end = seg_result.wal_end;

        if (seg_result.blob_segment_id > result.blob_segment_id or
            (seg_result.blob_segment_id == result.blob_segment_id and
                seg_result.blob_end > result.blob_end))
        {
            result.blob_segment_id = seg_result.blob_segment_id;
            result.blob_end = seg_result.blob_end;
        }

        if (seg_result.next_seqno > result.next_seqno) {
            result.next_seqno = seg_result.next_seqno;
        }

        wal_segment_id += 1;
    }

    return result;
}

pub fn replayWal(
    allocator: std.mem.Allocator,
    file: std.fs.File,
    blob_file: std.fs.File,
    index: *types.IndexMap,
) !ReplayResult {
    var result = ReplayResult{};
    var offset: u64 = 0;

    while (true) {
        var header: record.RecordHeader = undefined;

        const n = try file.pread(std.mem.asBytes(&header), offset);
        if (n == 0) break;
        if (n != @sizeOf(record.RecordHeader)) break;
        if (header.magic != record.RecordMagic) break;

        if (header.total_len < @sizeOf(record.RecordHeader) + 4) break;
        if (header.total_len > MaxReplayRecordSize) break;

        const total_len: usize = @intCast(header.total_len);

        var full_buf = try allocator.alloc(u8, total_len);
        defer allocator.free(full_buf);

        const read_full = try file.pread(full_buf, offset);
        if (read_full != total_len) break;

        const payload = full_buf[@sizeOf(record.RecordHeader)..];
        const computed_crc = record.crc32(payload);
        if (computed_crc != header.checksum) break;

        const rec_type: types.RecordType = @enumFromInt(header.record_type);
        const key_len: usize = @intCast(header.key_len);
        const value_len: usize = @intCast(header.value_len);

        const expected_total_len: usize = switch (rec_type) {
            .put_inline => @sizeOf(record.RecordHeader) + key_len + value_len + 4,
            .put_blob => @sizeOf(record.RecordHeader) + key_len + @sizeOf(types.BlobRef) + 4,
            .tombstone => @sizeOf(record.RecordHeader) + key_len + 4,
        };

        if (expected_total_len != total_len) break;

        const key_start = @sizeOf(record.RecordHeader);
        const key_end = key_start + key_len;
        if (key_end > full_buf.len) break;

        const key_src = full_buf[key_start..key_end];

        switch (rec_type) {
            .put_inline => {
                try upsertReplayValue(allocator, index, key_src, .{
                    .@"inline" = .{
                        .wal = .{
                            .segment_id = 0,
                            .offset = offset,
                            .len = header.total_len,
                        },
                        .seqno = header.seqno,
                    },
                });
            },
            .put_blob => {
                const blob_ref_start = key_end;
                const blob_ref_end = blob_ref_start + @sizeOf(types.BlobRef);
                if (blob_ref_end > full_buf.len) break;

                const blob_ref = std.mem.bytesToValue(
                    types.BlobRef,
                    full_buf[blob_ref_start..blob_ref_end],
                );

                if (blob_ref.total_len < @sizeOf(record.BlobHeader)) break;
                if (blob_ref.total_len > MaxReplayRecordSize) break;

                const ok = try validateBlob(allocator, blob_file, blob_ref);
                if (!ok) break;

                try upsertReplayValue(allocator, index, key_src, .{
                    .blob = .{
                        .blob = blob_ref,
                        .seqno = header.seqno,
                    },
                });

                const blob_end = blob_ref.offset + blob_ref.total_len;
                if (blob_end > result.blob_end) {
                    result.blob_end = blob_end;
                }
            },
            .tombstone => {
                try upsertReplayValue(allocator, index, key_src, .{
                    .tombstone = .{
                        .seqno = header.seqno,
                    },
                });
            },
        }

        const rec_end = offset + header.total_len;
        result.wal_end = rec_end;

        if (header.seqno + 1 > result.next_seqno) {
            result.next_seqno = header.seqno + 1;
        }

        offset = rec_end;
    }

    return result;
}
