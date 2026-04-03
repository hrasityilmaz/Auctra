const std = @import("std");
const types = @import("types.zig");

pub const RecordMagic: u32 = 0x48454C47; // HELG
pub const BlobMagic: u32 = 0x48424C42; // HBLB

pub const RecordHeader = packed struct {
    magic: u32,
    total_len: u32,
    checksum: u32,
    seqno: u64,
    key_len: u32,
    value_len: u32,
    record_type: u8,
    durability: u8,
    flags: u16,
};

pub const BlobHeader = packed struct {
    magic: u32,
    total_len: u32,
    checksum: u32,
    value_len: u32,
    flags: u32,
};

pub fn inlineWalSize(key_len: usize, value_len: usize) u32 {
    return @intCast(@sizeOf(RecordHeader) + key_len + value_len + 4);
}

pub fn blobWalSize(key_len: usize) u32 {
    return @intCast(@sizeOf(RecordHeader) + key_len + @sizeOf(types.BlobRef) + 4);
}

pub fn tombstoneWalSize(key_len: usize) u32 {
    return @intCast(@sizeOf(RecordHeader) + key_len + 4);
}

pub fn blobRecordSize(value_len: usize) u32 {
    return @intCast(@sizeOf(BlobHeader) + value_len + 4);
}

pub fn encodeInline(
    allocator: std.mem.Allocator,
    header_in: RecordHeader,
    key: []const u8,
    value: []const u8,
) ![]u8 {
    const total: usize = @intCast(header_in.total_len);
    var buf = try allocator.alloc(u8, total);
    errdefer allocator.free(buf);

    var header = header_in;
    header.checksum = 0;

    var fbs = std.io.fixedBufferStream(buf);
    const w = fbs.writer();

    try w.writeStruct(header);
    try w.writeAll(key);
    try w.writeAll(value);
    try w.writeInt(u32, 0, .little);

    const payload = buf[@sizeOf(RecordHeader)..];
    const crc = crc32(payload);
    std.mem.writeInt(u32, buf[8..12], crc, .little);

    return buf;
}

pub fn encodeBlobRef(
    allocator: std.mem.Allocator,
    header_in: RecordHeader,
    key: []const u8,
    blob_ref: types.BlobRef,
) ![]u8 {
    const total: usize = @intCast(header_in.total_len);
    var buf = try allocator.alloc(u8, total);
    errdefer allocator.free(buf);

    var header = header_in;
    header.checksum = 0;

    var fbs = std.io.fixedBufferStream(buf);
    const w = fbs.writer();

    try w.writeStruct(header);
    try w.writeAll(key);
    try w.writeStruct(blob_ref);
    try w.writeInt(u32, 0, .little);

    const payload = buf[@sizeOf(RecordHeader)..];
    const crc = crc32(payload);
    std.mem.writeInt(u32, buf[8..12], crc, .little);

    return buf;
}

pub fn encodeTombstone(
    allocator: std.mem.Allocator,
    header_in: RecordHeader,
    key: []const u8,
) ![]u8 {
    const total: usize = @intCast(header_in.total_len);
    var buf = try allocator.alloc(u8, total);
    errdefer allocator.free(buf);

    var header = header_in;
    header.checksum = 0;

    var fbs = std.io.fixedBufferStream(buf);
    const w = fbs.writer();

    try w.writeStruct(header);
    try w.writeAll(key);
    try w.writeInt(u32, 0, .little);

    const payload = buf[@sizeOf(RecordHeader)..];
    const crc = crc32(payload);
    std.mem.writeInt(u32, buf[8..12], crc, .little);

    return buf;
}

pub fn encodeBlobPayload(
    allocator: std.mem.Allocator,
    header_in: BlobHeader,
    value: []const u8,
) ![]u8 {
    const total: usize = @intCast(header_in.total_len);
    const buf = try allocator.alloc(u8, total);
    errdefer allocator.free(buf);

    var header = header_in;
    header.checksum = 0;

    var fbs = std.io.fixedBufferStream(buf);
    const w = fbs.writer();

    try w.writeStruct(header);
    try w.writeAll(value);
    try w.writeInt(u32, 0, .little);

    const payload = buf[@sizeOf(BlobHeader)..];
    const crc = crc32(payload);
    std.mem.writeInt(u32, buf[8..12], crc, .little);

    return buf;
}

pub fn crc32(data: []const u8) u32 {
    return std.hash.Crc32.hash(data);
}

pub fn crc32WithZeroTrailer(data: []const u8) u32 {
    var hasher = std.hash.Crc32.init();
    hasher.update(data);

    const zero_tail = [_]u8{ 0, 0, 0, 0 };
    hasher.update(&zero_tail);

    return hasher.final();
}

test "record sizes are monotonic" {
    std.fs.cwd().deleteFile("wal.log") catch {};
    std.fs.cwd().deleteFile("blob.log") catch {};
    try std.testing.expect(inlineWalSize(4, 4) > @sizeOf(RecordHeader));
    try std.testing.expect(blobWalSize(8) > @sizeOf(RecordHeader));
    try std.testing.expect(tombstoneWalSize(8) > @sizeOf(RecordHeader));
    try std.testing.expect(blobRecordSize(64) > @sizeOf(BlobHeader));
}
