const std = @import("std");

pub const protocol_version: u16 = 1;
pub const header_len: usize = 16;
pub const default_max_frame_size: u32 = 8 * 1024 * 1024; // 8 MiB

pub const Opcode = enum(u16) {
    ping = 0x0001,
    append = 0x0002,
    get = 0x0003,
    read_from = 0x0004,
    stats = 0x0005,

    pub fn fromInt(v: u16) ?Opcode {
        return std.meta.intToEnum(Opcode, v) catch null;
    }
};

pub const Status = enum(u16) {
    ok = 0,
    bad_request = 1,
    not_found = 2,
    internal_error = 3,
    unsupported_version = 4,
    too_large = 5,
    invalid_opcode = 6,
    invalid_cursor = 7,
    io_error = 8,
    busy = 9,
    unauthorized = 10,

    pub fn fromInt(v: u16) ?Status {
        return std.meta.intToEnum(Status, v) catch null;
    }
};

pub const FrameHeader = struct {
    frame_len: u32,
    version: u16,
    opcode: u16,
    request_id: u32,
    flags: u16,
    status: u16,

    pub fn init(opcode: Opcode, request_id: u32, status: Status, frame_len: u32) FrameHeader {
        return .{
            .frame_len = frame_len,
            .version = protocol_version,
            .opcode = @intFromEnum(opcode),
            .request_id = request_id,
            .flags = 0,
            .status = @intFromEnum(status),
        };
    }

    pub fn write(self: FrameHeader, writer: anytype) !void {
        var buf: [header_len]u8 = undefined;

        std.mem.writeInt(u32, buf[0..4], self.frame_len, .big);
        std.mem.writeInt(u16, buf[4..6], self.version, .big);
        std.mem.writeInt(u16, buf[6..8], self.opcode, .big);
        std.mem.writeInt(u32, buf[8..12], self.request_id, .big);
        std.mem.writeInt(u16, buf[12..14], self.flags, .big);
        std.mem.writeInt(u16, buf[14..16], self.status, .big);

        try writeAll(writer, &buf);
    }

    pub fn read(reader: anytype) !FrameHeader {
        var buf: [header_len]u8 = undefined;
        try readExact(reader, &buf);

        return .{
            .frame_len = std.mem.readInt(u32, buf[0..4], .big),
            .version = std.mem.readInt(u16, buf[4..6], .big),
            .opcode = std.mem.readInt(u16, buf[6..8], .big),
            .request_id = std.mem.readInt(u32, buf[8..12], .big),
            .flags = std.mem.readInt(u16, buf[12..14], .big),
            .status = std.mem.readInt(u16, buf[14..16], .big),
        };
    }
};

pub const PingResponse = struct {
    major: u16,
    minor: u16,
    patch: u16,
    reserved: u16 = 0,

    pub const encoded_len: usize = 8;

    pub fn write(self: PingResponse, writer: anytype) !void {
        var buf: [encoded_len]u8 = undefined;
        std.mem.writeInt(u16, buf[0..2], self.major, .big);
        std.mem.writeInt(u16, buf[2..4], self.minor, .big);
        std.mem.writeInt(u16, buf[4..6], self.patch, .big);
        std.mem.writeInt(u16, buf[6..8], self.reserved, .big);
        try writeAll(writer, &buf);
    }
};

pub const GetRequest = struct {
    key: []u8,

    pub fn decode(allocator: std.mem.Allocator, payload: []const u8) !GetRequest {
        if (payload.len < 2) return error.BadRequest;

        const key_len = std.mem.readInt(u16, payload[0..2], .big);
        if (payload.len != 2 + key_len) return error.BadRequest;
        if (key_len == 0) return error.BadRequest;

        const key = try allocator.alloc(u8, key_len);
        errdefer allocator.free(key);

        @memcpy(key, payload[2 .. 2 + key_len]);

        return .{
            .key = key,
        };
    }

    pub fn deinit(self: GetRequest, allocator: std.mem.Allocator) void {
        allocator.free(self.key);
    }
};

pub const GetResponse = struct {
    found: bool,
    value: []const u8,

    pub fn encode(self: GetResponse, allocator: std.mem.Allocator) ![]u8 {
        const total_len: usize = 1 + 1 + 2 + 4 + self.value.len;
        const out = try allocator.alloc(u8, total_len);
        errdefer allocator.free(out);

        out[0] = if (self.found) 1 else 0;
        out[1] = 0;
        std.mem.writeInt(u16, out[2..4], 0, .big);
        std.mem.writeInt(u32, out[4..8], @intCast(self.value.len), .big);

        if (self.value.len > 0) {
            @memcpy(out[8 .. 8 + self.value.len], self.value);
        }

        return out;
    }
};

pub const DurabilityMode = enum(u8) {
    ultrafast = 0,
    batch = 1,
    strict = 2,

    pub fn fromInt(v: u8) ?DurabilityMode {
        return std.meta.intToEnum(DurabilityMode, v) catch null;
    }
};

pub const AppendRequest = struct {
    durability: DurabilityMode,
    key: []u8,
    value: []u8,

    pub fn decode(allocator: std.mem.Allocator, payload: []const u8) !AppendRequest {
        if (payload.len < 8) return error.BadRequest;

        const durability_raw = payload[0];
        _ = payload[1]; // reserved
        const key_len = std.mem.readInt(u16, payload[2..4], .big);
        const value_len = std.mem.readInt(u32, payload[4..8], .big);

        const durability = DurabilityMode.fromInt(durability_raw) orelse return error.BadRequest;

        const expected_len: usize = 8 + key_len + value_len;
        if (payload.len != expected_len) return error.BadRequest;
        if (key_len == 0) return error.BadRequest;

        const key = try allocator.alloc(u8, key_len);
        errdefer allocator.free(key);

        const value = try allocator.alloc(u8, value_len);
        errdefer allocator.free(value);

        @memcpy(key, payload[8 .. 8 + key_len]);
        @memcpy(value, payload[8 + key_len .. expected_len]);

        return .{
            .durability = durability,
            .key = key,
            .value = value,
        };
    }

    pub fn deinit(self: AppendRequest, allocator: std.mem.Allocator) void {
        allocator.free(self.key);
        allocator.free(self.value);
    }
};

pub const AppendResponse = struct {
    commit_token: u64,

    pub fn encode(self: AppendResponse, allocator: std.mem.Allocator) ![]u8 {
        const out = try allocator.alloc(u8, 8);
        errdefer allocator.free(out);

        std.mem.writeInt(u64, out[0..8], self.commit_token, .big);
        return out;
    }
};

pub fn readExact(reader: anytype, buf: []u8) !void {
    var off: usize = 0;
    while (off < buf.len) {
        const n = try reader.read(buf[off..]);
        if (n == 0) return error.EndOfStream;
        off += n;
    }
}

pub fn writeAll(writer: anytype, buf: []const u8) !void {
    var off: usize = 0;
    while (off < buf.len) {
        const n = try writer.write(buf[off..]);
        if (n == 0) return error.WriteFailed;
        off += n;
    }
}

pub fn discardExact(reader: anytype, len: u32) !void {
    var remaining: usize = len;
    var tmp: [1024]u8 = undefined;

    while (remaining > 0) {
        const chunk_len: usize = @min(remaining, tmp.len);
        try readExact(reader, tmp[0..chunk_len]);
        remaining -= chunk_len;
    }
}

pub fn readPayloadAlloc(
    allocator: std.mem.Allocator,
    reader: anytype,
    len: u32,
    max_frame_size: u32,
) ![]u8 {
    if (len > max_frame_size) return error.FrameTooLarge;

    const payload = try allocator.alloc(u8, len);
    errdefer allocator.free(payload);

    try readExact(reader, payload);
    return payload;
}

pub fn writeHeaderAndPayload(
    writer: anytype,
    opcode: Opcode,
    request_id: u32,
    status: Status,
    payload: []const u8,
) !void {
    const hdr = FrameHeader.init(opcode, request_id, status, @intCast(payload.len));
    try hdr.write(writer);
    try writeAll(writer, payload);
}

pub fn writeErrorResponse(
    writer: anytype,
    request_id: u32,
    opcode: Opcode,
    status: Status,
) !void {
    const hdr = FrameHeader.init(opcode, request_id, status, 0);
    try hdr.write(writer);
}
