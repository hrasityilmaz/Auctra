const std = @import("std");
const hybrid = @import("hybrid_engine");

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
        while (seg < 16) : (seg += 1) {
            var wal_seg_buf: [40]u8 = undefined;
            var blob_seg_buf: [40]u8 = undefined;

            const wal_seg = std.fmt.bufPrint(&wal_seg_buf, "wal-{d:0>3}-{d:0>3}.log", .{ i, seg }) catch unreachable;
            const blob_seg = std.fmt.bufPrint(&blob_seg_buf, "blob-{d:0>3}-{d:0>3}.log", .{ i, seg }) catch unreachable;

            std.fs.cwd().deleteFile(wal_seg) catch {};
            std.fs.cwd().deleteFile(blob_seg) catch {};
        }
    }
}

test "snapshot empty engine roundtrip" {
    cleanupShardFiles(4);

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var engine = try hybrid.Engine.init(allocator, .{
        .shard_count = 4,
        .inline_max = 16,
    });
    defer engine.deinit();

    const path = "test-empty.snap";
    defer std.fs.cwd().deleteFile(path) catch {};
    defer cleanupShardFiles(4);

    try engine.writeStateSnapshotFile(allocator, path, .durable);

    const info = try hybrid.Engine.inspectStateSnapshotFile(allocator, path);
    try std.testing.expectEqual(@as(u32, 1), info.version);
    try std.testing.expectEqual(@as(u16, 4), info.shard_count);
    try std.testing.expectEqual(@as(u64, 0), info.entry_count);
}

test "snapshot restore roundtrip" {
    cleanupShardFiles(4);

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var engine = try hybrid.Engine.init(allocator, .{
        .shard_count = 4,
        .inline_max = 16,
    });
    defer engine.deinit();

    try engine.put(.strict, "user:1", "alice");
    try engine.put(.strict, "user:2", "bob");
    try engine.commitPending();

    const path = "test-restore.snap";
    defer std.fs.cwd().deleteFile(path) catch {};
    defer cleanupShardFiles(4);

    try engine.writeStateSnapshotFile(allocator, path, .durable);

    var engine2 = try hybrid.Engine.init(allocator, .{
        .shard_count = 4,
        .inline_max = 16,
    });
    defer engine2.deinit();

    const snap = try hybrid.Engine.readStateSnapshotFile(allocator, path);
    defer hybrid.Engine.freeStateSnapshot(allocator, snap);

    try engine2.restoreStateSnapshot(allocator, snap);

    const v1 = try engine2.readValue(allocator, "user:1");
    defer if (v1) |buf| allocator.free(buf);

    const v2 = try engine2.readValue(allocator, "user:2");
    defer if (v2) |buf| allocator.free(buf);

    try std.testing.expect(v1 != null);
    try std.testing.expect(v2 != null);
    try std.testing.expectEqualStrings("alice", v1.?);
    try std.testing.expectEqualStrings("bob", v2.?);
}

test "snapshot restore tombstone roundtrip" {
    cleanupShardFiles(4);

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var engine = try hybrid.Engine.init(allocator, .{
        .shard_count = 4,
        .inline_max = 16,
    });
    defer engine.deinit();

    try engine.put(.strict, "user:1", "alice");
    try engine.commitPending();

    try engine.del(.strict, "user:1");
    try engine.commitPending();

    const path = "test-restore-tombstone.snap";
    defer std.fs.cwd().deleteFile(path) catch {};
    defer cleanupShardFiles(4);

    try engine.writeStateSnapshotFile(allocator, path, .durable);

    var engine2 = try hybrid.Engine.init(allocator, .{
        .shard_count = 4,
        .inline_max = 16,
    });
    defer engine2.deinit();

    const snap = try hybrid.Engine.readStateSnapshotFile(allocator, path);
    defer hybrid.Engine.freeStateSnapshot(allocator, snap);

    try engine2.restoreStateSnapshot(allocator, snap);

    const v = try engine2.readValue(allocator, "user:1");
    defer if (v) |buf| allocator.free(buf);

    try std.testing.expect(v == null);
}

test "snapshot restore blob roundtrip" {
    cleanupShardFiles(4);

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var engine = try hybrid.Engine.init(allocator, .{
        .shard_count = 4,
        .inline_max = 8,
    });
    defer engine.deinit();

    const big_value = "this value is definitely larger than eight bytes";

    try engine.put(.strict, "user:blob", big_value);
    try engine.commitPending();

    const path = "test-restore-blob.snap";
    defer std.fs.cwd().deleteFile(path) catch {};
    defer cleanupShardFiles(4);

    try engine.writeStateSnapshotFile(allocator, path, .durable);

    var engine2 = try hybrid.Engine.init(allocator, .{
        .shard_count = 4,
        .inline_max = 8,
    });
    defer engine2.deinit();

    const snap = try hybrid.Engine.readStateSnapshotFile(allocator, path);
    defer hybrid.Engine.freeStateSnapshot(allocator, snap);

    try engine2.restoreStateSnapshot(allocator, snap);

    const v = try engine2.readValue(allocator, "user:blob");
    defer if (v) |buf| allocator.free(buf);

    try std.testing.expect(v != null);
    try std.testing.expectEqualStrings(big_value, v.?);
}

test "snapshot restore multi-shard roundtrip" {
    cleanupShardFiles(4);

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var engine = try hybrid.Engine.init(allocator, .{
        .shard_count = 4,
        .inline_max = 16,
    });
    defer engine.deinit();

    try engine.put(.strict, "user:1", "alice");
    try engine.put(.strict, "user:2", "bob");
    try engine.put(.strict, "order:1", "pending");
    try engine.put(.strict, "cart:1", "open");
    try engine.commitPending();

    const path = "test-restore-multishard.snap";
    defer std.fs.cwd().deleteFile(path) catch {};
    defer cleanupShardFiles(4);

    try engine.writeStateSnapshotFile(allocator, path, .durable);

    var engine2 = try hybrid.Engine.init(allocator, .{
        .shard_count = 4,
        .inline_max = 16,
    });
    defer engine2.deinit();

    const snap = try hybrid.Engine.readStateSnapshotFile(allocator, path);
    defer hybrid.Engine.freeStateSnapshot(allocator, snap);

    try engine2.restoreStateSnapshot(allocator, snap);

    const v1 = try engine2.readValue(allocator, "user:1");
    defer if (v1) |buf| allocator.free(buf);

    const v2 = try engine2.readValue(allocator, "user:2");
    defer if (v2) |buf| allocator.free(buf);

    const v3 = try engine2.readValue(allocator, "order:1");
    defer if (v3) |buf| allocator.free(buf);

    const v4 = try engine2.readValue(allocator, "cart:1");
    defer if (v4) |buf| allocator.free(buf);

    try std.testing.expect(v1 != null);
    try std.testing.expect(v2 != null);
    try std.testing.expect(v3 != null);
    try std.testing.expect(v4 != null);

    try std.testing.expectEqualStrings("alice", v1.?);
    try std.testing.expectEqualStrings("bob", v2.?);
    try std.testing.expectEqualStrings("pending", v3.?);
    try std.testing.expectEqualStrings("open", v4.?);
}

test "snapshot truncated file is rejected" {
    cleanupShardFiles(2);

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var engine = try hybrid.Engine.init(allocator, .{ .shard_count = 2, .inline_max = 16 });
    defer engine.deinit();

    try engine.put(.strict, "user:1", "alice");
    try engine.commitPending();

    const path = "test-truncated.snap";
    defer std.fs.cwd().deleteFile(path) catch {};
    defer cleanupShardFiles(2);

    try engine.writeStateSnapshotFile(allocator, path, .durable);

    var file = try std.fs.cwd().openFile(path, .{ .mode = .read_write });
    defer file.close();
    const stat = try file.stat();
    try file.setEndPos(stat.size - 3);

    try std.testing.expectError(error.InvalidSnapshotFile, hybrid.Engine.readStateSnapshotFile(allocator, path));
}

test "snapshot checksum mismatch is rejected" {
    cleanupShardFiles(2);

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var engine = try hybrid.Engine.init(allocator, .{ .shard_count = 2, .inline_max = 16 });
    defer engine.deinit();

    try engine.put(.strict, "user:1", "alice");
    try engine.commitPending();

    const path = "test-bad-checksum.snap";
    defer std.fs.cwd().deleteFile(path) catch {};
    defer cleanupShardFiles(2);

    try engine.writeStateSnapshotFile(allocator, path, .durable);

    var file = try std.fs.cwd().openFile(path, .{ .mode = .read_write });
    defer file.close();
    try file.seekTo(36);
    var byte: [1]u8 = undefined;
    _ = try file.read(&byte);
    try file.seekBy(-1);
    byte[0] ^= 0xFF;
    try file.writeAll(&byte);

    try std.testing.expectError(error.SnapshotChecksumMismatch, hybrid.Engine.readStateSnapshotFile(allocator, path));
}
