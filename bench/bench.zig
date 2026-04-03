const std = @import("std");
const hybrid = @import("hybrid_engine");

const PayloadMode = enum {
    small,
    value_1k,
};

fn cleanupShardFiles(shard_count: usize) void {
    var i: usize = 0;
    while (i < shard_count) : (i += 1) {
        var wal_name_buf: [32]u8 = undefined;
        var blob_name_buf: [32]u8 = undefined;

        const wal_name = std.fmt.bufPrint(&wal_name_buf, "wal-{d:0>3}.log", .{i}) catch unreachable;
        const blob_name = std.fmt.bufPrint(&blob_name_buf, "blob-{d:0>3}.log", .{i}) catch unreachable;

        std.fs.cwd().deleteFile(wal_name) catch {};
        std.fs.cwd().deleteFile(blob_name) catch {};

        var seg: usize = 1;
        while (seg < 64) : (seg += 1) {
            var wal_seg_buf: [40]u8 = undefined;
            var blob_seg_buf: [40]u8 = undefined;

            const wal_seg = std.fmt.bufPrint(&wal_seg_buf, "wal-{d:0>3}-{d:0>3}.log", .{ i, seg }) catch unreachable;
            const blob_seg = std.fmt.bufPrint(&blob_seg_buf, "blob-{d:0>3}-{d:0>3}.log", .{ i, seg }) catch unreachable;

            std.fs.cwd().deleteFile(wal_seg) catch {};
            std.fs.cwd().deleteFile(blob_seg) catch {};
        }
    }
}

fn throughputPerSec(count: usize, elapsed_ms: i64) usize {
    const safe_ms: usize = if (elapsed_ms <= 0) 1 else @intCast(elapsed_ms);
    return (count * 1000) / safe_ms;
}

fn printSection(comptime title: []const u8) void {
    std.debug.print("{s}\n", .{title});
}

fn payloadModeName(mode: PayloadMode) []const u8 {
    return switch (mode) {
        .small => "small-inline",
        .value_1k => "value-1k",
    };
}

fn fillValueBuffer(buf: []u8, i: usize, mode: PayloadMode) ![]const u8 {
    return switch (mode) {
        .small => try std.fmt.bufPrint(buf, "value:{d}", .{i}),
        .value_1k => blk: {
            if (buf.len < 1024) return error.BufferTooSmall;

            @memset(buf[0..1024], 'x');

            const prefix = try std.fmt.bufPrint(buf[0..32], "value:{d}|", .{i});
            @memcpy(buf[0..prefix.len], prefix);

            break :blk buf[0..1024];
        },
    };
}

fn runAppendBench(
    allocator: std.mem.Allocator,
    shard_count: usize,
    total_ops: usize,
    durability: hybrid.Durability,
    inline_max: u32,
    mode: PayloadMode,
) !void {
    cleanupShardFiles(shard_count);
    defer cleanupShardFiles(shard_count);

    var engine = try hybrid.Engine.init(allocator, .{
        .shard_count = @intCast(shard_count),
        .inline_max = inline_max,
    });
    defer engine.deinit();

    const start_ms = std.time.milliTimestamp();

    var i: usize = 0;
    while (i < total_ops) : (i += 1) {
        var key_buf: [32]u8 = undefined;
        var val_buf: [1024]u8 = undefined;

        const key = try std.fmt.bufPrint(&key_buf, "user:{d}", .{i});
        const value = try fillValueBuffer(&val_buf, i, mode);

        try engine.put(durability, key, value);
    }

    try engine.commitPending();

    const end_ms = std.time.milliTimestamp();
    const elapsed_ms = end_ms - start_ms;

    std.debug.print("  payload: {s}\n", .{payloadModeName(mode)});
    std.debug.print("  inline_max: {d}\n", .{inline_max});
    std.debug.print("  durability: {s}\n", .{
        switch (durability) {
            .ultrafast => "ultrafast",
            .batch => "batch",
            .strict => "strict",
        },
    });
    std.debug.print("  ops: {d}\n", .{total_ops});
    std.debug.print("  time: {d} ms\n", .{elapsed_ms});
    std.debug.print("  throughput: {d} ops/s\n\n", .{
        throughputPerSec(total_ops, elapsed_ms),
    });
}

fn runMergedReplayBench(
    allocator: std.mem.Allocator,
    shard_count: usize,
    total_ops: usize,
    page_size: usize,
    mode: PayloadMode,
    inline_max: u32,
) !void {
    cleanupShardFiles(shard_count);
    defer cleanupShardFiles(shard_count);

    var engine = try hybrid.Engine.init(allocator, .{
        .shard_count = @intCast(shard_count),
        .inline_max = inline_max,
    });
    defer engine.deinit();

    var i: usize = 0;
    while (i < total_ops) : (i += 1) {
        var key_buf: [32]u8 = undefined;
        var val_buf: [1024]u8 = undefined;

        const key = try std.fmt.bufPrint(&key_buf, "user:{d}", .{i});
        const value = try fillValueBuffer(&val_buf, i, mode);

        try engine.put(.strict, key, value);
    }
    try engine.commitPending();

    var cursor = try engine.initGlobalMergeCursor(allocator);
    defer hybrid.Engine.freeGlobalMergeCursor(allocator, cursor);

    var total_read: usize = 0;
    const read_start_ms = std.time.milliTimestamp();

    while (true) {
        const page = try engine.readFromAllMerged(allocator, cursor, page_size);

        const next_cursor = try allocator.dupe(hybrid.ShardCursor, page.next_cursor.shard_cursors);
        hybrid.Engine.freeGlobalMergeCursor(allocator, cursor);
        cursor = .{ .shard_cursors = next_cursor };

        total_read += page.items.len;

        const item_count = page.items.len;
        hybrid.Engine.freeGlobalMergeReadFromResult(allocator, page);

        if (item_count == 0) break;
    }

    const read_end_ms = std.time.milliTimestamp();
    const elapsed_ms = read_end_ms - read_start_ms;

    std.debug.print("  payload: {s}\n", .{payloadModeName(mode)});
    std.debug.print("  inline_max: {d}\n", .{inline_max});
    std.debug.print("  items: {d}\n", .{total_read});
    std.debug.print("  page_size: {d}\n", .{page_size});
    std.debug.print("  time: {d} ms\n", .{elapsed_ms});
    std.debug.print("  throughput: {d} items/s\n\n", .{
        throughputPerSec(total_read, elapsed_ms),
    });
}

fn runSnapshotSaveBench(
    allocator: std.mem.Allocator,
    shard_count: usize,
    total_ops: usize,
    mode: PayloadMode,
    inline_max: u32,
) !void {
    cleanupShardFiles(shard_count);
    defer cleanupShardFiles(shard_count);

    var engine = try hybrid.Engine.init(allocator, .{
        .shard_count = @intCast(shard_count),
        .inline_max = inline_max,
    });
    defer engine.deinit();

    var i: usize = 0;
    while (i < total_ops) : (i += 1) {
        var key_buf: [32]u8 = undefined;
        var val_buf: [1024]u8 = undefined;

        const key = try std.fmt.bufPrint(&key_buf, "user:{d}", .{i});
        const value = try fillValueBuffer(&val_buf, i, mode);

        try engine.put(.strict, key, value);
    }
    try engine.commitPending();

    const path = "bench.snap";
    defer std.fs.cwd().deleteFile(path) catch {};

    const start_ms = std.time.milliTimestamp();
    try engine.writeStateSnapshotFile(allocator, path, .durable);
    const end_ms = std.time.milliTimestamp();

    std.debug.print("  payload: {s}\n", .{payloadModeName(mode)});
    std.debug.print("  inline_max: {d}\n", .{inline_max});
    std.debug.print("  entries: {d}\n", .{total_ops});
    std.debug.print("  time: {d} ms\n\n", .{end_ms - start_ms});
}

fn runSnapshotRestoreBench(
    allocator: std.mem.Allocator,
    shard_count: usize,
    total_ops: usize,
    mode: PayloadMode,
    inline_max: u32,
) !void {
    cleanupShardFiles(shard_count);
    defer cleanupShardFiles(shard_count);

    var engine = try hybrid.Engine.init(allocator, .{
        .shard_count = @intCast(shard_count),
        .inline_max = inline_max,
    });
    defer engine.deinit();

    var i: usize = 0;
    while (i < total_ops) : (i += 1) {
        var key_buf: [32]u8 = undefined;
        var val_buf: [1024]u8 = undefined;

        const key = try std.fmt.bufPrint(&key_buf, "user:{d}", .{i});
        const value = try fillValueBuffer(&val_buf, i, mode);

        try engine.put(.strict, key, value);
    }
    try engine.commitPending();

    const path = "bench.snap";
    defer std.fs.cwd().deleteFile(path) catch {};
    try engine.writeStateSnapshotFile(allocator, path, .durable);

    var engine2 = try hybrid.Engine.init(allocator, .{
        .shard_count = @intCast(shard_count),
        .inline_max = inline_max,
    });
    defer engine2.deinit();

    const snap = try hybrid.Engine.readStateSnapshotFile(allocator, path);
    defer hybrid.Engine.freeStateSnapshot(allocator, snap);

    const start_ms = std.time.milliTimestamp();
    try engine2.restoreStateSnapshot(allocator, snap);
    const end_ms = std.time.milliTimestamp();

    std.debug.print("  payload: {s}\n", .{payloadModeName(mode)});
    std.debug.print("  inline_max: {d}\n", .{inline_max});
    std.debug.print("  entries: {d}\n", .{total_ops});
    std.debug.print("  time: {d} ms\n\n", .{end_ms - start_ms});
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const shard_count: usize = 4;
    const total_ops: usize = 100_000;

    printSection("APPEND (SMALL)");
    try runAppendBench(allocator, shard_count, total_ops, .ultrafast, 32, .small);
    try runAppendBench(allocator, shard_count, total_ops, .batch, 32, .small);
    try runAppendBench(allocator, shard_count, total_ops, .strict, 32, .small);

    printSection("APPEND (1K VALUE)");
    try runAppendBench(allocator, shard_count, total_ops, .ultrafast, 32, .value_1k);
    try runAppendBench(allocator, shard_count, total_ops, .batch, 32, .value_1k);
    try runAppendBench(allocator, shard_count, total_ops, .strict, 32, .value_1k);

    printSection("MERGED REPLAY (SMALL)");
    try runMergedReplayBench(allocator, shard_count, total_ops, 256, .small, 32);

    printSection("MERGED REPLAY (1K VALUE)");
    try runMergedReplayBench(allocator, shard_count, total_ops, 256, .value_1k, 32);

    printSection("SNAPSHOT SAVE (SMALL)");
    try runSnapshotSaveBench(allocator, shard_count, total_ops, .small, 32);

    printSection("SNAPSHOT SAVE (1K VALUE)");
    try runSnapshotSaveBench(allocator, shard_count, total_ops, .value_1k, 32);

    printSection("SNAPSHOT RESTORE (SMALL)");
    try runSnapshotRestoreBench(allocator, shard_count, total_ops, .small, 32);

    printSection("SNAPSHOT RESTORE (1K VALUE)");
    try runSnapshotRestoreBench(allocator, shard_count, total_ops, .value_1k, 32);
}
