const std = @import("std");
const hybrid = @import("hybrid_engine");

test "checkpoint roundtrip and replay" {
    var engine = try hybrid.Engine.init(std.testing.allocator, .{ .shard_count = 4, .inline_max = 1024 });
    defer engine.deinit();

    _ = try engine.appendBatchAndCommit(0, &[_]hybrid.types.BatchOp{.{ .put = .{ .key = "a", .value = "1" } }}, .{ .durability = .strict, .await_durable = true });
    _ = try engine.appendBatchAndCommit(1, &[_]hybrid.types.BatchOp{.{ .put = .{ .key = "b", .value = "2" } }}, .{ .durability = .strict, .await_durable = true });

    const cp = try engine.captureCheckpoint(std.testing.allocator, .durable);
    defer hybrid.Engine.freeCheckpoint(std.testing.allocator, cp);
    try std.testing.expectEqual(@as(usize, 4), cp.shard_cursors.len);

    const path = "test-checkpoint.cecp";
    defer std.fs.cwd().deleteFile(path) catch {};
    try hybrid.Engine.writeCheckpointFileFromValue(path, cp);

    const loaded = try hybrid.Engine.readCheckpointFile(std.testing.allocator, path);
    defer hybrid.Engine.freeCheckpoint(std.testing.allocator, loaded);
    try std.testing.expectEqual(cp.shard_count, loaded.shard_count);
    try std.testing.expectEqual(cp.high_seqno, loaded.high_seqno);

    const merged_cursor = try engine.checkpointToGlobalMergeCursor(std.testing.allocator, loaded);
    defer hybrid.Engine.freeGlobalMergeCursor(std.testing.allocator, merged_cursor);

    const result = try engine.readFromAllMergedConsistent(std.testing.allocator, merged_cursor, 16, .durable);
    defer hybrid.Engine.freeGlobalMergeReadFromResult(std.testing.allocator, result);
    try std.testing.expectEqual(@as(usize, 0), result.items.len);
}

test "empty checkpoint is readable" {
    var engine = try hybrid.Engine.init(std.testing.allocator, .{ .shard_count = 2, .inline_max = 128 });
    defer engine.deinit();

    const cp = try engine.captureCheckpoint(std.testing.allocator, .durable);
    defer hybrid.Engine.freeCheckpoint(std.testing.allocator, cp);
    try std.testing.expectEqual(@as(usize, 2), cp.shard_cursors.len);
}

test "checkpoint truncated file is rejected" {
    var engine = try hybrid.Engine.init(std.testing.allocator, .{ .shard_count = 2, .inline_max = 128 });
    defer engine.deinit();

    _ = try engine.appendBatchAndCommit(0, &[_]hybrid.types.BatchOp{.{ .put = .{ .key = "a", .value = "1" } }}, .{ .durability = .strict, .await_durable = true });

    const cp = try engine.captureCheckpoint(std.testing.allocator, .durable);
    defer hybrid.Engine.freeCheckpoint(std.testing.allocator, cp);

    const path = "test-checkpoint-truncated.cecp";
    defer std.fs.cwd().deleteFile(path) catch {};
    try hybrid.Engine.writeCheckpointFileFromValue(path, cp);

    var file = try std.fs.cwd().openFile(path, .{ .mode = .read_write });
    defer file.close();
    const stat = try file.stat();
    try file.setEndPos(stat.size - 2);

    try std.testing.expectError(error.InvalidCheckpointFile, hybrid.Engine.readCheckpointFile(std.testing.allocator, path));
}

test "checkpoint checksum mismatch is rejected" {
    var engine = try hybrid.Engine.init(std.testing.allocator, .{ .shard_count = 2, .inline_max = 128 });
    defer engine.deinit();

    const cp = try engine.captureCheckpoint(std.testing.allocator, .durable);
    defer hybrid.Engine.freeCheckpoint(std.testing.allocator, cp);

    const path = "test-checkpoint-bad-checksum.cecp";
    defer std.fs.cwd().deleteFile(path) catch {};
    try hybrid.Engine.writeCheckpointFileFromValue(path, cp);

    var file = try std.fs.cwd().openFile(path, .{ .mode = .read_write });
    defer file.close();
    try file.seekTo(28);
    var byte: [1]u8 = undefined;
    _ = try file.read(&byte);
    try file.seekBy(-1);
    byte[0] ^= 0xFF;
    try file.writeAll(&byte);

    try std.testing.expectError(error.CheckpointChecksumMismatch, hybrid.Engine.readCheckpointFile(std.testing.allocator, path));
}
