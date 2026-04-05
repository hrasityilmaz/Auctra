const std = @import("std");
const hybrid_api = @import("../hybrid_api.zig");
const wire = @import("../wire/protocol.zig");
const dispatch = @import("dispatch.zig");

pub const ServerState = struct {
    db: hybrid_api.HybridDb,
    started_at_unix_s: u64,

    pub fn init(allocator: std.mem.Allocator) !ServerState {
        return .{
            .db = try hybrid_api.HybridDb.init(allocator, .{}),
            .started_at_unix_s = @intCast(std.time.timestamp()),
        };
    }

    pub fn deinit(self: *ServerState) void {
        self.db.deinit();
    }

    pub fn uptimeSeconds(self: *const ServerState) u64 {
        const now: u64 = @intCast(std.time.timestamp());
        return now - self.started_at_unix_s;
    }
};

pub fn run(allocator: std.mem.Allocator, address: []const u8) !void {
    var state = try ServerState.init(allocator);
    defer state.deinit();

    const parsed = try std.net.Address.parseIp(address, 7001);

    var server = try parsed.listen(.{
        .reuse_address = true,
    });
    defer server.deinit();

    std.log.info("Auctra Server listening on {s}:{}", .{ address, 7001 });

    while (true) {
        var conn = try server.accept();
        defer conn.stream.close();

        std.log.info("client connected", .{});

        handleConnection(allocator, &state, &conn) catch |err| {
            switch (err) {
                error.EndOfStream => std.log.info("client disconnected", .{}),
                error.ConnectionResetByPeer => std.log.warn("connection reset by peer", .{}),
                error.BrokenPipe => std.log.warn("broken pipe", .{}),
                error.FrameTooLarge => std.log.warn("frame too large; closing connection", .{}),
                else => std.log.err("connection error: {}", .{err}),
            }
        };
    }
}

fn handleConnection(
    allocator: std.mem.Allocator,
    state: *ServerState,
    conn: *std.net.Server.Connection,
) !void {
    while (true) {
        const header = try wire.FrameHeader.read(&conn.stream);
        try dispatch.handleFrame(allocator, state, &conn.stream, &conn.stream, header);
    }
}
