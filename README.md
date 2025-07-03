### Write-Ahead Log (WAL) for Raft.

-----------------
This crate provides a durable, append-only log optimized for use with Raft.

It uses direct I/O (`O_DIRECT`) to bypass the OS page cache, ensuring consistent write latency
and avoiding double-buffering.

Entries are written in page-aligned frames and are never modified in-place.
All writes are strictly append-only, preserving the integrity of existing data.

The WAL consists of immutable segments and a mutable head segment. Older segments are read
sequentially; new entries are appended to the writable head segment.

sequentially; new entries are appended to the writable head segment.

This is not a production-ready project. It was built as a proof of concept to explore
how to design a write-ahead log for Raft-style systems while minimizing redundant memory usage.

The design is inspired by etcd's WAL,
which maintains both a write-ahead log and an in-memory state of entries that haven't yet
been snapshotted. The goal here is to replicate the core WAL behavior while eliminating
double buffering by relying on O_DIRECT to write directly to disk and letting upper layers
handle any needed in-memory state.

This implementation does not handle batching, snapshotting, or in-memory caching.
Those responsibilities are delegated to higher-level components. This crate focuses solely
on the correctness and efficiency of low-level wal persistence.
