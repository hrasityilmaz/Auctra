const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const lib_mod = b.createModule(.{
        .root_source_file = b.path("src/lib.zig"),
        .target = target,
        .optimize = optimize,
    });

    const exe_mod = b.createModule(.{
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
    });

    const exe = b.addExecutable(.{
        .name = "auctra-core",
        .root_module = exe_mod,
    });
    exe.root_module.addImport("hybrid_engine", lib_mod);
    b.installArtifact(exe);

    const shared_mod = b.createModule(.{
        .root_source_file = b.path("src/c_api.zig"),
        .target = target,
        .optimize = optimize,
    });
    shared_mod.addImport("hybrid_engine", lib_mod);

    const shared = b.addLibrary(.{
        .name = "auctra_core",
        .root_module = shared_mod,
        .linkage = .dynamic,
    });
    shared.linkLibC();
    b.installArtifact(shared);

    const bench_mod = b.createModule(.{
        .root_source_file = b.path("bench/bench.zig"),
        .target = target,
        .optimize = optimize,
    });
    bench_mod.addImport("hybrid_engine", lib_mod);

    const bench = b.addExecutable(.{
        .name = "bench",
        .root_module = bench_mod,
    });
    b.installArtifact(bench);

    const tests = b.addTest(.{
        .root_module = lib_mod,
    });

    const test_step = b.step("test", "Run library tests");
    const run_tests = b.addRunArtifact(tests);
    test_step.dependOn(&run_tests.step);

    const checkpoint_test_mod = b.createModule(.{
        .root_source_file = b.path("test/checkpoint_test.zig"),
        .target = target,
        .optimize = optimize,
    });
    checkpoint_test_mod.addImport("hybrid_engine", lib_mod);

    const checkpoint_tests = b.addTest(.{
        .root_module = checkpoint_test_mod,
    });

    const run_checkpoint_tests = b.addRunArtifact(checkpoint_tests);
    test_step.dependOn(&run_checkpoint_tests.step);

    const snapshot_test_mod = b.createModule(.{
        .root_source_file = b.path("test/snapshot_test.zig"),
        .target = target,
        .optimize = optimize,
    });
    snapshot_test_mod.addImport("hybrid_engine", lib_mod);

    const snapshot_tests = b.addTest(.{
        .root_module = snapshot_test_mod,
    });

    const run_snapshot_tests = b.addRunArtifact(snapshot_tests);
    test_step.dependOn(&run_snapshot_tests.step);
}
