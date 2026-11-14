"""Local and S3 filesystem helpers via pyarrow only."""

from __future__ import annotations

import json
import os
from typing import Any

import pyarrow as pa
import pyarrow.fs as pafs
import pyarrow.parquet as pq


def is_s3(uri: str) -> bool:
    return uri.startswith("s3://")


def join_uri(root: str, *parts: str) -> str:
    root = root.rstrip("/")
    cleaned = [p.strip("/") for p in parts if p is not None and str(p) != ""]
    if not cleaned:
        return root
    return "/".join([root, *cleaned])


def open_fs(uri: str) -> tuple[pafs.FileSystem, str]:
    if is_s3(uri):
        region = os.environ.get("AWS_REGION", "us-east-1")
        fs = pafs.S3FileSystem(region=region)
        path = uri[len("s3://") :]
        return fs, path
    return pafs.LocalFileSystem(), uri


def write_parquet(table: pa.Table, uri: str) -> None:
    fs, path = open_fs(uri)
    if not is_s3(uri):
        parent = os.path.dirname(path)
        if parent:
            os.makedirs(parent, exist_ok=True)
    pq.write_table(table, path, filesystem=fs, compression="snappy")


def read_parquet(uri: str) -> pa.Table:
    fs, path = open_fs(uri)
    return pq.read_table(path, filesystem=fs)


def write_json(obj: Any, uri: str) -> None:
    fs, path = open_fs(uri)
    if not is_s3(uri):
        parent = os.path.dirname(path)
        if parent:
            os.makedirs(parent, exist_ok=True)
    data = json.dumps(obj, sort_keys=True).encode("utf-8")
    with fs.open_output_stream(path) as out:
        out.write(data)


def read_json(uri: str) -> Any:
    fs, path = open_fs(uri)
    with fs.open_input_stream(path) as inp:
        return json.loads(inp.read().decode("utf-8"))


def exists(uri: str) -> bool:
    fs, path = open_fs(uri)
    info = fs.get_file_info(path)
    return info.type != pafs.FileType.NotFound


def list_files(prefix_uri: str) -> list[str]:
    fs, path = open_fs(prefix_uri)
    path = path.rstrip("/")
    selector = pafs.FileSelector(path, recursive=True, allow_not_found=True)
    try:
        infos = fs.get_file_info(selector)
    except FileNotFoundError:
        return []
    results: list[str] = []
    for info in infos:
        if info.type != pafs.FileType.File:
            continue
        if is_s3(prefix_uri):
            results.append(f"s3://{info.path}")
        else:
            results.append(info.path)
    return sorted(results)


def delete_prefix(prefix_uri: str) -> None:
    fs, path = open_fs(prefix_uri)
    path = path.rstrip("/")
    selector = pafs.FileSelector(path, recursive=True, allow_not_found=True)
    try:
        infos = fs.get_file_info(selector)
    except FileNotFoundError:
        return
    for info in sorted(infos, key=lambda i: i.path, reverse=True):
        if info.type == pafs.FileType.File:
            fs.delete_file(info.path)
        elif info.type == pafs.FileType.Directory:
            try:
                fs.delete_dir(info.path)
            except OSError:
                pass
    try:
        info = fs.get_file_info(path)
        if info.type == pafs.FileType.Directory:
            fs.delete_dir(path)
    except OSError:
        pass


def dir_size_bytes(prefix_uri: str) -> int:
    total = 0
    for file_uri in list_files(prefix_uri):
        fs, path = open_fs(file_uri)
        info = fs.get_file_info(path)
        if info.type == pafs.FileType.File and info.size is not None:
            total += int(info.size)
    return total


def to_spark_uri(uri: str) -> str:
    if uri.startswith("s3://"):
        return "s3a://" + uri[len("s3://") :]
    return uri
