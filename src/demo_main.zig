const std = @import("std");
const api = @import("hybrid_api.zig");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer {
        const status = gpa.deinit();
        if (status == .leak) std.log.err("memory leak detected", .{});
    }
    const allocator = gpa.allocator();

    const snapshot_path = "demo-snapshot.bin";

    var db = try api.AuctraDb.init(allocator, .{});
    defer db.deinit();

    std.debug.print("Auctra Core demo\n", .{});
    std.debug.print("==================\n\n", .{});

    std.debug.print("1) create user:1 => Alice\n", .{});
    try db.put("user:1", "Alice");

    {
        const value = try db.get("user:1");
        defer value.deinit(allocator);
        std.debug.print("   current value: {s}\n\n", .{value.bytes});
    }

    std.debug.print("2) update user:1 => Bob\n", .{});
    try db.put("user:1", "Bob");

    {
        const value = try db.get("user:1");
        defer value.deinit(allocator);
        std.debug.print("   current value: {s}\n\n", .{value.bytes});
    }

    std.debug.print("3) create user:2 => Carol\n", .{});
    try db.put("user:2", "Carol");

    {
        const value1 = try db.get("user:1");
        defer value1.deinit(allocator);
        const value2 = try db.get("user:2");
        defer value2.deinit(allocator);

        std.debug.print("   user:1 = {s}\n", .{value1.bytes});
        std.debug.print("   user:2 = {s}\n\n", .{value2.bytes});
    }

    std.debug.print("4) save snapshot => {s}\n", .{snapshot_path});
    try db.snapshot(snapshot_path);
    std.debug.print("   snapshot saved\n\n", .{});

    std.debug.print("5) mutate state after snapshot\n", .{});
    try db.put("user:1", "Eve");
    const deleted = try db.delete("user:2");
    std.debug.print("   deleted user:2 = {s}\n", .{if (deleted) "true" else "false"});

    {
        const value = try db.get("user:1");
        defer value.deinit(allocator);
        std.debug.print("   current user:1 = {s}\n", .{value.bytes});
    }
    std.debug.print("   exists user:2 = {s}\n\n", .{if (db.exists("user:2")) "true" else "false"});

    std.debug.print("6) restore snapshot\n", .{});
    try db.restore(snapshot_path);
    std.debug.print("   restore complete\n\n", .{});

    {
        const value1 = try db.get("user:1");
        defer value1.deinit(allocator);

        std.debug.print("7) verify restored state\n", .{});
        std.debug.print("   user:1 = {s}\n", .{value1.bytes});
        std.debug.print("   exists user:2 = {s}\n", .{if (db.exists("user:2")) "true" else "false"});

        const ok_user1 = std.mem.eql(u8, value1.bytes, "Bob");
        const ok_user2 = db.exists("user:2");

        if (ok_user1 and ok_user2) {
            std.debug.print("\nstate correct ✅\n", .{});
        } else {
            std.debug.print("\nstate incorrect ❌\n", .{});
            return error.DemoVerificationFailed;
        }
    }

    std.fs.cwd().deleteFile(snapshot_path) catch {};
}
