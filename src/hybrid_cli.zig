const std = @import("std");
const api = @import("hybrid_api.zig");
const types = @import("types.zig");

pub fn printUsage() void {
    std.debug.print(
        \\usage:
        \\  hybrid-engine demo
        \\  hybrid-engine put <key> <value> [ultrafast|batch|strict]
        \\  hybrid-engine get <key>
        \\  hybrid-engine delete <key> [ultrafast|batch|strict]
        \\  hybrid-engine exists <key>
        \\  hybrid-engine stats
        \\  hybrid-engine frontier
        \\  hybrid-engine inspect
        \\  hybrid-engine checkpoint save <path> [visible|durable]
        \\  hybrid-engine checkpoint inspect <path>
        \\  hybrid-engine snapshot save <path> [visible|durable]
        \\  hybrid-engine snapshot inspect <path>
        \\  hybrid-engine snapshot restore <path>
        \\
        \\demo:
        \\  Runs a built-in showcase that appends data, saves a snapshot,
        \\  mutates state, restores the snapshot, verifies correctness,
        \\  and prints rough timings in ns/us/ms.
        \\
    , .{});
}

pub fn parseDurability(arg: ?[]const u8) types.Durability {
    if (arg) |s| {
        if (std.mem.eql(u8, s, "ultrafast")) return .ultrafast;
        if (std.mem.eql(u8, s, "batch")) return .batch;
        if (std.mem.eql(u8, s, "strict")) return .strict;
    }
    return .strict;
}

pub fn parseCheckpointKind(arg: ?[]const u8) types.CheckpointKind {
    if (arg) |s| {
        if (std.mem.eql(u8, s, "visible")) return .visible;
        if (std.mem.eql(u8, s, "durable")) return .durable;
    }
    return .durable;
}

pub fn handleTopLevel(
    db: *api.HybridDb,
    allocator: std.mem.Allocator,
    cmd: []const u8,
    args: *std.process.ArgIterator,
) !bool {
    _ = allocator;

    if (std.mem.eql(u8, cmd, "put")) {
        const key = args.next() orelse return error.InvalidArguments;
        const value = args.next() orelse return error.InvalidArguments;
        const durability = parseDurability(args.next());
        try db.putWithDurability(durability, key, value);
        std.debug.print("ok put key={s} durability={s}\n", .{ key, @tagName(durability) });
        return true;
    }

    if (std.mem.eql(u8, cmd, "get")) {
        const key = args.next() orelse return error.InvalidArguments;
        const value = db.get(key) catch |err| switch (err) {
            api.KeyNotFound.KeyNotFound => {
                std.debug.print("not_found key={s}\n", .{key});
                return true;
            },
            else => return err,
        };
        defer value.deinit(db.allocator);
        std.debug.print("{s}\n", .{value.bytes});
        return true;
    }

    if (std.mem.eql(u8, cmd, "delete")) {
        const key = args.next() orelse return error.InvalidArguments;
        const durability = parseDurability(args.next());
        const existed = try db.deleteWithDurability(durability, key);
        std.debug.print(
            "deleted={s} key={s} durability={s}\n",
            .{ if (existed) "true" else "false", key, @tagName(durability) },
        );
        return true;
    }

    if (std.mem.eql(u8, cmd, "exists")) {
        const key = args.next() orelse return error.InvalidArguments;
        std.debug.print("{s}\n", .{if (db.exists(key)) "true" else "false"});
        return true;
    }

    return false;
}
