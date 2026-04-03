const std = @import("std");
const builtin = @import("builtin");
const linux = std.os.linux;
const backend = @import("backend.zig");

pub const UringWriter = struct {
    ring: linux.IoUring,

    pub fn init(entries: u16) !UringWriter {
        if (builtin.os.tag != .linux) return error.UnsupportedPlatform;

        return .{
            .ring = try linux.IoUring.init(entries, 0),
        };
    }

    pub fn deinit(self: *UringWriter) void {
        self.ring.deinit();
    }

    pub fn writeAt(
        self: *UringWriter,
        fd: std.posix.fd_t,
        data: []const u8,
        offset: u64,
    ) !void {
        var fake_file = std.fs.File{ .handle = fd };
        const one = [_]backend.WriteOp{
            .{
                .file = &fake_file,
                .offset = offset,
                .data = data,
            },
        };
        try self.writeBatch(&one);
    }

    pub fn writeBatch(self: *UringWriter, ops: []const backend.WriteOp) !void {
        if (ops.len == 0) return;

        var submitted: usize = 0;

        for (ops, 0..) |op, i| {
            if (op.data.len == 0) continue;

            const sqe = try self.ring.get_sqe();
            sqe.prep_write(op.file.handle, op.data, op.offset);
            sqe.user_data = i;
            submitted += 1;
        }

        if (submitted == 0) return;

        _ = try self.ring.submit();

        const cqes = try std.heap.page_allocator.alloc(linux.io_uring_cqe, submitted);
        defer std.heap.page_allocator.free(cqes);

        const wait_nr: u32 = std.math.cast(u32, submitted) orelse return error.TooManyOperations;
        const count = try self.ring.copy_cqes(cqes, wait_nr);
        if (count != wait_nr) return error.ShortCompletion;

        for (cqes[0..count]) |cqe| {
            if (cqe.res < 0) {
                return std.posix.unexpectedErrno(@enumFromInt(-cqe.res));
            }

            const op_index: usize = @intCast(cqe.user_data);
            if (op_index >= ops.len) return error.InvalidCompletionIndex;

            const op = ops[op_index];
            const wrote: usize = @intCast(cqe.res);

            if (wrote > op.data.len) return error.InvalidWriteResult;

            if (wrote < op.data.len) {
                try op.file.pwriteAll(op.data[wrote..], op.offset + wrote);
            }
        }
    }
};
