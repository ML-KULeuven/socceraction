"""FSSpec adapter for filesystem operations."""

import os
import re
import shutil
from typing import BinaryIO, Optional

import fsspec
from fsspec.spec import AbstractFileSystem


class FSSpecAdapter:
    """Adapter for interacting with filesystems using fsspec.

    This adapter provides a unified interface for reading, writing, and
    querying files and directories across local, remote (e.g., S3, GCS, HTTP),
    and in-memory filesystems supported by fsspec.

    Parameters
    ----------
    storage_options : dict, optional
        Extra options passed to the underlying fsspec filesystem backend.
    cache_storage : str, optional
        Path to local directory for caching remote files. If None, caching
        is disabled.
    """

    def __init__(
        self,
        storage_options: Optional[dict[str, object]] = None,
        cache_storage: Optional[str] = None,
    ) -> None:
        self.storage_options = storage_options or {}
        self.cache_storage = cache_storage

    def _infer_protocol(self, url: str) -> str:
        """Infer the protocol based on the URL prefix.

        Parameters
        ----------
        url : str
            The URL or file path.

        Returns
        -------
        str
            The inferred protocol (e.g. 's3', 'memory', 'file').
        """
        protocol_pattern = re.compile(r"^[a-zA-Z\d]+://")
        match = protocol_pattern.match(url)
        if match:
            return match.group(0)[:-3]
        return "file"

    def _get_filesystem(self, url: str, no_cache: bool = False) -> AbstractFileSystem:
        """Get the appropriate fsspec filesystem for the given URL.

        Parameters
        ----------
        url : str
            The URL or file path.
        no_cache : bool, default False
            Whether to bypass caching.

        Returns
        -------
        fsspec.spec.AbstractFileSystem
            The configured fsspec filesystem instance.
        """
        protocol = self._infer_protocol(url)
        if self.cache_storage and not no_cache and protocol not in ("file", "memory"):
            return fsspec.filesystem(
                "simplecache",
                target_protocol=protocol,
                target_options=self.storage_options,
                cache_storage=self.cache_storage,
            )
        return fsspec.filesystem(protocol, **self.storage_options)

    def _get_filesystem_for_reading(self, url: str) -> AbstractFileSystem:
        """Get filesystem instance configured for reading.

        Parameters
        ----------
        url : str
            The URL or file path.

        Returns
        -------
        fsspec.spec.AbstractFileSystem
            The filesystem instance for reading.
        """
        return self._get_filesystem(url, no_cache=False)

    def _get_filesystem_for_writing(self, url: str) -> AbstractFileSystem:
        """Get filesystem instance configured for writing.

        Parameters
        ----------
        url : str
            The URL or file path.

        Returns
        -------
        fsspec.spec.AbstractFileSystem
            The filesystem instance for writing.
        """
        return self._get_filesystem(url, no_cache=True)

    def _detect_compression(self, url: str) -> Optional[str]:
        """Detect the compression type based on the file extension.

        Parameters
        ----------
        url : str
            The URL or file path.

        Returns
        -------
        str or None
            The compression type (e.g. 'gzip', 'bz2') or None.
        """
        compression_map = {
            ".gz": "gzip",
            ".bz2": "bz2",
            ".xz": "xz",
            ".zip": "zip",
        }
        for ext, comp in compression_map.items():
            if url.endswith(ext):
                return comp
        return None

    def supports(self, url: str) -> bool:
        """Check if the adapter can handle the given URL.

        Parameters
        ----------
        url : str
            The URL or file path to check.

        Returns
        -------
        bool
            True if the protocol is supported, False otherwise.
        """
        protocol = self._infer_protocol(url)
        try:
            fsspec.get_filesystem_class(protocol)
            return True
        except (ValueError, ImportError):
            return False

    def open(
        self,
        url: str,
        mode: str = "rb",
        compression: Optional[str] = None,
        **kwargs: object,
    ) -> BinaryIO:
        """Open a file from the given URL.

        Parameters
        ----------
        url : str
            The URL or file path.
        mode : str, default 'rb'
            Mode in which to open the file.
        compression : str, optional
            Compression type. If None, it is auto-detected from the file extension.
        **kwargs
            Additional keyword arguments passed to filesystem open.

        Returns
        -------
        BinaryIO
            The opened file object.
        """
        fs = (
            self._get_filesystem_for_writing(url)
            if ("w" in mode or "a" in mode)
            else self._get_filesystem_for_reading(url)
        )
        if compression is None:
            compression = self._detect_compression(url)
        return fs.open(url, mode=mode, compression=compression, **kwargs)

    def read_to_stream(self, url: str, output: BinaryIO) -> None:
        """Read content from the given URL and write it to the provided stream.

        Parameters
        ----------
        url : str
            The source URL.
        output : BinaryIO
            Stream to write data into.

        Raises
        ------
        FileNotFoundError
            If the source file does not exist.
        """
        fs = self._get_filesystem_for_reading(url)
        compression = self._detect_compression(url)
        with fs.open(url, "rb", compression=compression) as source_file:
            shutil.copyfileobj(source_file, output)

    def write_from_stream(self, url: str, input: BinaryIO, mode: str = "wb") -> None:  # noqa: A002
        """Write content from the input stream to the destination URL.

        Parameters
        ----------
        url : str
            The destination URL.
        input : BinaryIO
            Stream to read data from.
        mode : str, default 'wb'
            Write mode ('wb' for overwrite or 'ab' for append).
        """
        fs = self._get_filesystem_for_writing(url)
        compression = self._detect_compression(url)
        with fs.open(url, mode=mode, compression=compression) as dest_file:
            shutil.copyfileobj(input, dest_file)

    def list_directory(self, url: str, recursive: bool = True) -> list[str]:
        """List the contents of a directory.

        Parameters
        ----------
        url : str
            The directory URL.
        recursive : bool, default True
            Whether to list files recursively.

        Returns
        -------
        list of str
            List of URLs/file paths contained in the directory.
        """
        protocol = self._infer_protocol(url)
        fs = self._get_filesystem_for_reading(url)
        try:
            if recursive:
                files = fs.find(url, detail=False)
            else:
                files = fs.listdir(url, detail=False)
        except FileNotFoundError:
            return []

        results: list[str] = []
        for fp in files:
            file_str = fp.get("name", "") if isinstance(fp, dict) else str(fp)
            if protocol == "file":
                results.append(os.path.normpath(file_str))
            else:
                if not file_str.startswith(f"{protocol}://"):
                    results.append(f"{protocol}://{file_str.lstrip('/')}")
                else:
                    results.append(file_str)
        return results

    def is_directory(self, url: str) -> bool:
        """Check if the given URL points to a directory.

        Parameters
        ----------
        url : str
            The URL to check.

        Returns
        -------
        bool
            True if URL is a directory, False otherwise.
        """
        fs = self._get_filesystem_for_reading(url)
        try:
            return bool(fs.isdir(url))
        except Exception:
            return False

    def is_file(self, url: str) -> bool:
        """Check if the given URL points to a file.

        Parameters
        ----------
        url : str
            The URL to check.

        Returns
        -------
        bool
            True if URL is a file, False otherwise.
        """
        fs = self._get_filesystem_for_reading(url)
        try:
            return bool(fs.isfile(url))
        except Exception:
            return False

    def exists(self, url: str) -> bool:
        """Check if the given URL exists.

        Parameters
        ----------
        url : str
            The URL to check.

        Returns
        -------
        bool
            True if URL exists, False otherwise.
        """
        fs = self._get_filesystem_for_reading(url)
        try:
            return bool(fs.exists(url))
        except Exception:
            return False

    def makedirs(self, url: str, exist_ok: bool = True) -> None:
        """Create a directory and any parent directories.

        Parameters
        ----------
        url : str
            The directory URL to create.
        exist_ok : bool, default True
            If True, no exception is raised if the directory already exists.
        """
        fs = self._get_filesystem_for_writing(url)
        fs.makedirs(url, exist_ok=exist_ok)

    def rm(self, url: str, recursive: bool = False) -> None:
        """Remove a file or directory.

        Parameters
        ----------
        url : str
            The URL to remove.
        recursive : bool, default False
            Whether to recursively remove directory contents.
        """
        fs = self._get_filesystem_for_writing(url)
        fs.rm(url, recursive=recursive)
