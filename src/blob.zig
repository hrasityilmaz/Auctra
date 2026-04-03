const std = @import("std");
const record = @import("record.zig");
const types = @import("types.zig");

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

pub fn readBlobHeader(
    file: std.fs.File,
    offset: u64,
) !record.BlobHeader {
    var header: record.BlobHeader = undefined;
    const n = try file.pread(std.mem.asBytes(&header), offset);
    if (n != @sizeOf(record.BlobHeader)) return error.UnexpectedEof;
    return header;
}

pub fn validateBlobRef(
    allocator: std.mem.Allocator,
    file: std.fs.File,
    blob_ref: types.BlobRef,
) !bool {
    _ = allocator;

    if (blob_ref.total_len < @sizeOf(record.BlobHeader)) return false;

    const header = readBlobHeader(file, blob_ref.offset) catch return false;

    if (header.magic != record.BlobMagic) return false;
    if (header.total_len != blob_ref.total_len) return false;
    if (header.value_len != blob_ref.value_len) return false;
    if (header.checksum != blob_ref.checksum) return false;

    const payload_len: usize = @intCast(header.total_len - @sizeOf(record.BlobHeader));
    if (payload_len < 4) return false;
    if (header.value_len + 4 != payload_len) return false;

    var payload = try std.heap.page_allocator.alloc(u8, payload_len);
    defer std.heap.page_allocator.free(payload);

    const n = try file.pread(payload, blob_ref.offset + @sizeOf(record.BlobHeader));
    if (n != payload_len) return false;

    const computed = record.crc32(payload);
    if (computed != header.checksum) return false;

    const value_len: usize = @intCast(header.value_len);
    const trailer = payload[value_len..];
    return std.mem.eql(u8, trailer, &[_]u8{ 0, 0, 0, 0 });
}

pub fn readBlobValue(
    allocator: std.mem.Allocator,
    file: std.fs.File,
    blob_ref: types.BlobRef,
) ![]u8 {
    if (!(try validateBlobRef(allocator, file, blob_ref))) {
        return error.ChecksumMismatch;
    }

    const header = try readBlobHeader(file, blob_ref.offset);
    const payload_len: usize = @intCast(header.total_len - @sizeOf(record.BlobHeader));

    var payload = try allocator.alloc(u8, payload_len);
    defer allocator.free(payload);

    const n = try file.pread(payload, blob_ref.offset + @sizeOf(record.BlobHeader));
    if (n != payload_len) return error.UnexpectedEof;

    if (payload_len < 4) return error.InvalidBlobPayload;

    const value_len: usize = @intCast(header.value_len);
    if (value_len + 4 != payload_len) return error.InvalidBlobPayload;

    const trailer = payload[value_len..];
    if (!std.mem.eql(u8, trailer, &[_]u8{ 0, 0, 0, 0 })) {
        return error.InvalidBlobTrailer;
    }

    return try allocator.dupe(u8, payload[0..value_len]);
}

pub fn copyLiveBlob(
    allocator: std.mem.Allocator,
    src_file: std.fs.File,
    dst_file: std.fs.File,
    dst_segment_id: u32,
    dst_blob_end: *u64,
    blob_ref: types.BlobRef,
) !types.BlobRef {
    if (!(try validateBlobRef(allocator, src_file, blob_ref))) {
        return error.ChecksumMismatch;
    }

    const total_len: usize = @intCast(blob_ref.total_len);
    const full_buf = try allocator.alloc(u8, total_len);
    defer allocator.free(full_buf);

    const n = try src_file.pread(full_buf, blob_ref.offset);
    if (n != total_len) return error.UnexpectedEof;

    try dst_file.pwriteAll(full_buf, dst_blob_end.*);

    const new_ref = types.BlobRef{
        .segment_id = dst_segment_id,
        .offset = dst_blob_end.*,
        .total_len = blob_ref.total_len,
        .value_len = blob_ref.value_len,
        .checksum = blob_ref.checksum,
    };

    dst_blob_end.* += blob_ref.total_len;
    return new_ref;
}

pub const Blob = struct {
    file: std.fs.File,
    path: []const u8,

    pub fn openOrCreate(allocator: std.mem.Allocator, path: []const u8) !Blob {
        const owned_path = try allocator.dupe(u8, path);
        errdefer allocator.free(owned_path);

        const cwd = std.fs.cwd();

        _ = cwd.createFile(path, .{
            .read = true,
            .truncate = false,
            .exclusive = true,
        }) catch |err| switch (err) {
            error.PathAlreadyExists => {},
            else => return err,
        };

        const file = try cwd.openFile(path, .{ .mode = .read_write });

        return .{
            .file = file,
            .path = owned_path,
        };
    }

    pub fn openShardSegment(
        allocator: std.mem.Allocator,
        shard_id: u16,
        segment_id: u32,
    ) !Blob {
        const path = try allocBlobSegmentPath(allocator, shard_id, segment_id);
        defer allocator.free(path);
        return openOrCreate(allocator, path);
    }

    pub fn openShard(allocator: std.mem.Allocator, shard_id: u16) !Blob {
        return openShardSegment(allocator, shard_id, 0);
    }

    pub fn close(self: *Blob, allocator: std.mem.Allocator) void {
        self.file.close();
        allocator.free(self.path);
    }

    pub fn writeAt(self: *Blob, offset: u64, data: []const u8) !void {
        var written: usize = 0;
        while (written < data.len) {
            const n = try self.file.pwrite(data[written..], offset + written);
            written += n;
        }
    }

    pub fn syncData(self: *Blob) !void {
        try self.file.sync();
    }
};
