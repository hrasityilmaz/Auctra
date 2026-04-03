const std = @import("std");
const hybrid = @import("hybrid_engine");

fn userKey(
    allocator: std.mem.Allocator,
    user_id: []const u8,
) ![]u8 {
    return try std.fmt.allocPrint(allocator, "user:{s}", .{user_id});
}

const UserState = struct {
    id: []u8,
    email: []u8,
    name: []u8,
};

fn freeUserState(allocator: std.mem.Allocator, state: UserState) void {
    allocator.free(state.id);
    allocator.free(state.email);
    allocator.free(state.name);
}

fn encodeUserValue(
    allocator: std.mem.Allocator,
    email: []const u8,
    name: []const u8,
) ![]u8 {
    return try std.fmt.allocPrint(
        allocator,
        "email={s}|name={s}",
        .{ email, name },
    );
}

fn decodeUserState(
    allocator: std.mem.Allocator,
    user_id: []const u8,
    value: []const u8,
) !UserState {
    var email = try allocator.dupe(u8, "");
    errdefer allocator.free(email);

    var name = try allocator.dupe(u8, "");
    errdefer allocator.free(name);

    var parts = std.mem.splitScalar(u8, value, '|');
    while (parts.next()) |part| {
        if (std.mem.startsWith(u8, part, "email=")) {
            allocator.free(email);
            email = try allocator.dupe(u8, part["email=".len..]);
        } else if (std.mem.startsWith(u8, part, "name=")) {
            allocator.free(name);
            name = try allocator.dupe(u8, part["name=".len..]);
        }
    }

    return .{
        .id = try allocator.dupe(u8, user_id),
        .email = email,
        .name = name,
    };
}

fn createUser(
    allocator: std.mem.Allocator,
    engine: *hybrid.Engine,
    shard_id: u16,
    user_id: []const u8,
    email: []const u8,
    name: []const u8,
) !hybrid.AppendBatchAndCommitResult {
    const key = try userKey(allocator, user_id);
    defer allocator.free(key);

    const value = try encodeUserValue(allocator, email, name);
    defer allocator.free(value);

    const ops = [_]hybrid.BatchOp{
        .{
            .put = .{
                .key = key,
                .value = value,
            },
        },
    };

    return try engine.appendBatchAndCommit(
        shard_id,
        &ops,
        .{
            .await_durable = true,
        },
    );
}

fn updateUserEmail(
    allocator: std.mem.Allocator,
    engine: *hybrid.Engine,
    shard_id: u16,
    user_id: []const u8,
    new_email: []const u8,
    name: []const u8,
) !hybrid.AppendBatchAndCommitResult {
    const key = try userKey(allocator, user_id);
    defer allocator.free(key);

    const value = try encodeUserValue(allocator, new_email, name);
    defer allocator.free(value);

    const ops = [_]hybrid.BatchOp{
        .{
            .put = .{
                .key = key,
                .value = value,
            },
        },
    };

    return try engine.appendBatchAndCommit(
        shard_id,
        &ops,
        .{
            .await_durable = true,
        },
    );
}

fn deleteUser(
    allocator: std.mem.Allocator,
    engine: *hybrid.Engine,
    shard_id: u16,
    user_id: []const u8,
) !hybrid.AppendBatchAndCommitResult {
    const key = try userKey(allocator, user_id);
    defer allocator.free(key);

    const ops = [_]hybrid.BatchOp{
        .{
            .del = .{
                .key = key,
            },
        },
    };

    return try engine.appendBatchAndCommit(
        shard_id,
        &ops,
        .{
            .await_durable = true,
        },
    );
}

fn getCurrentUser(
    allocator: std.mem.Allocator,
    engine: *hybrid.Engine,
    user_id: []const u8,
) !?UserState {
    const key = try userKey(allocator, user_id);
    defer allocator.free(key);

    const value = try engine.readValue(allocator, key);
    if (value == null) return null;
    defer allocator.free(value.?);

    return try decodeUserState(allocator, user_id, value.?);
}

fn getUserAt(
    allocator: std.mem.Allocator,
    engine: *hybrid.Engine,
    cursor: hybrid.Cursor,
    user_id: []const u8,
) !?UserState {
    const key = try userKey(allocator, user_id);
    defer allocator.free(key);

    const value = try engine.getAt(allocator, cursor, key);
    if (value == null) return null;
    defer allocator.free(value.?);

    return try decodeUserState(allocator, user_id, value.?);
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var engine = try hybrid.Engine.init(allocator, .{
        .shard_count = 4,
        .inline_max = 64,
    });
    defer engine.deinit();

    const shard_id: u16 = 0;

    std.debug.print("== create user ==\n", .{});
    const created = try createUser(
        allocator,
        &engine,
        shard_id,
        "42",
        "alice@example.com",
        "Alice",
    );

    std.debug.print(
        "window: count={} first_seq={} last_seq={}\n",
        .{
            created.window.record_count,
            created.window.first_seqno,
            created.window.last_seqno,
        },
    );

    std.debug.print("== update email ==\n", .{});
    _ = try updateUserEmail(
        allocator,
        &engine,
        shard_id,
        "42",
        "alice@newdomain.com",
        "Alice",
    );

    std.debug.print("== current state ==\n", .{});
    const current = try getCurrentUser(allocator, &engine, "42");
    if (current) |user| {
        defer freeUserState(allocator, user);
        std.debug.print(
            "current: id={s} email={s} name={s}\n",
            .{ user.id, user.email, user.name },
        );
    } else {
        std.debug.print("current: null\n", .{});
    }

    std.debug.print("== snapshot at create window ==\n", .{});
    const snap = try getUserAt(
        allocator,
        &engine,
        .{
            .shard_id = shard_id,
            .wal_segment_id = created.window.end.wal_segment_id,
            .wal_offset = created.window.end.wal_offset,
        },
        "42",
    );

    if (snap) |user| {
        defer freeUserState(allocator, user);
        std.debug.print(
            "snapshot: id={s} email={s} name={s}\n",
            .{ user.id, user.email, user.name },
        );
    } else {
        std.debug.print("snapshot: null\n", .{});
    }

    std.debug.print("== delete user ==\n", .{});
    _ = try deleteUser(allocator, &engine, shard_id, "42");

    const after_delete = try getCurrentUser(allocator, &engine, "42");
    if (after_delete) |user| {
        defer freeUserState(allocator, user);
        std.debug.print(
            "after delete: id={s} email={s} name={s}\n",
            .{ user.id, user.email, user.name },
        );
    } else {
        std.debug.print("after delete: null\n", .{});
    }
}
