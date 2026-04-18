# @cervid/decomposer

High-performance binary data decomposer for the Cervid ecosystem.

`@cervid/decomposer` is the low-level ingestion layer for binary and columnar formats such as:

- NPY
- Parquet
- Arrow
- Avro
- other binary-oriented formats

Its job is to read source files and expose them in a shared-memory-friendly representation that can later be consumed by other Cervid libraries, such as `@cervid/data`.

## Status

> **This package is currently under construction.**
>
> The API may change while the core architecture stabilizes.

At the moment, the main focus is:

- NPY ingestion
- shared-memory output
- stable interchange contract between Decomposer and Data

## Goals

- Fast binary ingestion
- SharedArrayBuffer-based output
- Zero-copy or near-zero-copy interoperability
- Clean bridge to `@cervid/data`
- Future format exporters (`toCSV`, `toJSON`, `toNPY`, `toParquet`, etc.)

## Installation

```bash
npm install @cervid/decomposer