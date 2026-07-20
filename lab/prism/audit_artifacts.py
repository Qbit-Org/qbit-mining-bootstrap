#!/usr/bin/env python3
"""Filesystem ownership, verification, and publication for PRISM audit data.

This module deliberately has no coordinator dependency.  The coordinator owns
block-finalization sequencing and the ledger owns database authorization; this
store is the sole authority for paths and filesystem mutation below the audit
root.
"""

from __future__ import annotations

import copy
from contextlib import contextmanager, nullcontext
import fcntl
import hashlib
import hmac
import json
import os
import re
import selectors
import signal
import stat
import subprocess
import threading
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterator, Mapping

from lab.prism.prism_tools import prism_tool_command


AUDIT_BODY_REF_SCHEMA = "qbit.prism.audit-body-ref.v1"
AUDIT_BUNDLE_V2_SCHEMA = "qbit.prism.audit-bundle.v2"
AUDIT_SHARE_SEGMENT_SCHEMA = "qbit.prism.audit-share-segment.v1"
AUDIT_WINDOW_COMPLETENESS_PROOF_SCHEMA = (
    "qbit.prism.window-completeness-proof.v1"
)
LIVE_ENVELOPE_SCHEMA = "qbit.prism.live-audit-bundle-envelope.v1"
LIVE_EVIDENCE_SCHEMA = "qbit.prism.live-stratum-evidence.v1"
DEFAULT_AUDIT_SHARE_SEGMENT_SIZE = 10_000
DEFAULT_VERIFIER_TIMEOUT_SECONDS = 60.0
MAX_VERIFIER_OUTPUT_BYTES = 1024 * 1024
VERIFICATION_REPORT_SCHEMA = "qbit.prism.audit-verification-report.v1"
VERIFICATION_IDENTITY_SCHEMA = "qbit.prism.audit-verification-identity.v1"
LEGACY_VERIFICATION_UNAVAILABLE_SCHEMA = (
    "qbit.prism.legacy-verification-unavailable.v1"
)

_HEX_64 = r"[0-9a-f]{64}"
_TOKEN_32 = r"[0-9a-f]{32}"
_BODY_RE = re.compile(
    rf"\Aprism-audit-bundle-body-(?P<block>{_HEX_64})-(?P<digest>{_HEX_64})\.json\Z"
)
_SHARE_CONTENT_RE = re.compile(
    rf"\Aprism-audit-share-segment-(?P<first>[1-9][0-9]*)-"
    rf"(?P<last>[1-9][0-9]*)-(?P<digest>{_HEX_64})\.json\Z"
)
_SHARE_SLOT_RE = re.compile(
    r"\Aprism-audit-share-segment-slot-(?P<first>[1-9][0-9]*)-"
    r"(?P<last>[1-9][0-9]*)\.json\Z"
)
_LIVE_RE = re.compile(
    rf"\Aprism-live-audit-bundle-(?P<height>0|[1-9][0-9]*)-"
    rf"(?P<block>{_HEX_64})\.json\Z"
)
_CANDIDATE_RE = re.compile(
    rf"\A\.prism-live-audit-bundle-candidate-(?P<block>{_HEX_64})-"
    rf"(?P<token>{_TOKEN_32})\.json\.tmp\Z"
)
_LEGACY_CANDIDATE_RE = re.compile(
    rf"\Aprism-live-audit-bundle-candidate-(?P<block>{_HEX_64})\.json\Z"
)
_LEGACY_HIDDEN_CANDIDATE_RE = re.compile(
    rf"\A\.prism-live-audit-bundle-candidate-(?P<block>{_HEX_64})\.json\.tmp\Z"
)
_PUBLICATION_LOCK_NAME = ".prism-audit-publication.lock"
_PUBLICATION_THREAD_LOCKS_GUARD = threading.Lock()
_PUBLICATION_THREAD_LOCKS: dict[tuple[int, int], threading.RLock] = {}
_PUBLICATION_LOCAL_OWNERS: dict[tuple[int, int], tuple[int, int]] = {}


def _publication_thread_lock(identity: tuple[int, int]) -> threading.RLock:
    """Share a local lock across store instances opened on the same inode."""

    with _PUBLICATION_THREAD_LOCKS_GUARD:
        lock = _PUBLICATION_THREAD_LOCKS.get(identity)
        if lock is None:
            lock = threading.RLock()
            _PUBLICATION_THREAD_LOCKS[identity] = lock
        return lock


def _reject_cross_store_local_owner(
    identity: tuple[int, int],
    *,
    thread_id: int,
    store_id: int,
) -> None:
    """Reject same-thread nesting through a second store before it can block."""

    with _PUBLICATION_THREAD_LOCKS_GUARD:
        local_owner = _PUBLICATION_LOCAL_OWNERS.get(identity)
    if local_owner is not None:
        owner_thread, owner_store = local_owner
        if owner_thread == thread_id and owner_store != store_id:
            raise RuntimeError(
                "audit publication guard is already held by another store "
                "on this thread"
            )


def _canonical_hex(value: object, *, name: str, expected_bytes: int = 32) -> str:
    text = str(value)
    if len(text) != expected_bytes * 2:
        raise ValueError(f"{name} must be {expected_bytes} bytes of hex")
    try:
        bytes.fromhex(text)
    except ValueError as exc:
        raise ValueError(f"{name} must be hexadecimal") from exc
    return text.lower()


def _sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _json_bytes(payload: Mapping[str, Any], *, sort_keys: bool = False) -> bytes:
    return json.dumps(
        payload,
        separators=(",", ":"),
        sort_keys=sort_keys,
    ).encode("utf-8")


def canonical_audit_bundle_bytes(
    final_bundle: dict[str, Any],
    canonicalizer: Callable[[dict[str, Any]], bytes] | None = None,
) -> bytes:
    if canonicalizer is None:
        raise RuntimeError("J1 canonical bundle capability is required")
    canonical = canonicalizer(final_bundle)
    return canonical.encode() if isinstance(canonical, str) else bytes(canonical)


@dataclass(frozen=True)
class AuditArtifactConfig:
    root: Path
    evidence_path: Path
    live_bundle_retention: int = 5
    candidate_retention_seconds: int = 24 * 60 * 60
    share_segment_size: int = 0
    verifier_timeout_seconds: float = DEFAULT_VERIFIER_TIMEOUT_SECONDS


@dataclass(frozen=True)
class AuditPublicationIdentity:
    """Ordering token assigned while P1's balance serializer is held."""

    sequence: int
    block_height: int
    block_hash: str

    def __post_init__(self) -> None:
        if isinstance(self.sequence, bool) or not isinstance(self.sequence, int):
            raise ValueError("publication sequence must be an integer")
        if self.sequence < 0:
            raise ValueError("publication sequence must be non-negative")
        if isinstance(self.block_height, bool) or not isinstance(
            self.block_height,
            int,
        ):
            raise ValueError("publication block height must be an integer")
        if self.block_height < 0:
            raise ValueError("publication block height must be non-negative")
        canonical_block_hash = _canonical_hex(
            self.block_hash,
            name="publication block hash",
        )
        if self.block_hash != canonical_block_hash:
            raise ValueError("publication block hash must be canonical")

    def to_json(self) -> dict[str, object]:
        return {
            "sequence": self.sequence,
            "block_height": self.block_height,
            "block_hash": self.block_hash,
        }


@dataclass(frozen=True)
class OwnedCandidateArtifact:
    path: Path
    block_hash: str
    token: str
    store_token: str


@dataclass(frozen=True)
class VerifiedAuditBundle:
    candidate: OwnedCandidateArtifact
    report: Mapping[str, Any]
    literal_sha256: str
    byte_length: int
    device: int
    inode: int
    mtime_ns: int
    canonical_copy_eligible: bool
    verification_identity: Mapping[str, Any]


@dataclass(frozen=True)
class PublishedAuditBodyRef:
    path: Path
    body_uri: str
    audit_bundle_sha256: str


@dataclass(frozen=True)
class AuditPublication:
    identity: AuditPublicationIdentity
    envelope_path: Path
    evidence: Mapping[str, Any]
    published: bool


@dataclass(frozen=True)
class RetentionResult:
    live_removed: int = 0
    candidate_removed: int = 0
    errors: int = 0


@dataclass(frozen=True)
class _FileIdentity:
    device: int
    inode: int
    mode: int
    size: int
    mtime_ns: int

    @classmethod
    def from_stat(cls, value: os.stat_result) -> _FileIdentity:
        return cls(
            value.st_dev,
            value.st_ino,
            value.st_mode,
            value.st_size,
            value.st_mtime_ns,
        )

    def matches(self, value: os.stat_result) -> bool:
        return (
            self.device == value.st_dev
            and self.inode == value.st_ino
            and self.mode == value.st_mode
            and self.size == value.st_size
            and self.mtime_ns == value.st_mtime_ns
            and stat.S_ISREG(value.st_mode)
        )


@dataclass(frozen=True)
class _LegacyProofToken:
    identity: AuditPublicationIdentity
    evidence_file: _FileIdentity
    evidence_sha256: str
    envelope_file: _FileIdentity
    envelope_sha256: str


class AuditArtifactStore:
    """Own every audit artifact path and filesystem mutation for one root."""

    def __init__(
        self,
        config: AuditArtifactConfig,
        *,
        canonicalizer: Callable[[dict[str, Any]], bytes] | None = None,
        verifier: Callable[..., dict[str, Any]] | None = None,
        wall_time: Callable[[], float] = time.time,
    ) -> None:
        live_bundle_retention = int(config.live_bundle_retention)
        candidate_retention_seconds = int(config.candidate_retention_seconds)
        share_segment_size = int(config.share_segment_size)
        verifier_timeout_seconds = float(config.verifier_timeout_seconds)
        if share_segment_size < 0:
            raise ValueError("audit share segment size must be non-negative")
        if verifier_timeout_seconds <= 0:
            raise ValueError("verifier timeout must be positive")
        configured_root = Path(config.root).expanduser().absolute()
        configured_root.mkdir(parents=True, exist_ok=True)
        root_stat = configured_root.lstat()
        if not stat.S_ISDIR(root_stat.st_mode) or stat.S_ISLNK(root_stat.st_mode):
            raise RuntimeError("audit artifact root must be a non-symlink directory")
        root = configured_root.resolve(strict=True)
        configured_evidence = Path(config.evidence_path).expanduser().absolute()
        configured_evidence.parent.mkdir(parents=True, exist_ok=True)
        evidence_path = configured_evidence.parent.resolve(strict=True) / configured_evidence.name
        self._root = root
        self._evidence_path = evidence_path
        self._root_fd, self._root_identity = self._open_directory_authority(root)
        try:
            (
                self._publication_lock_fd,
                self._publication_lock_identity,
            ) = self._open_publication_lock_authority(
                root,
                self._root_fd,
                self._root_identity,
            )
            self._publication_lifecycle_lock = threading.RLock()
            self._publication_inode_thread_lock = _publication_thread_lock(
                (
                    self._publication_lock_identity.device,
                    self._publication_lock_identity.inode,
                )
            )
            self._publication_guard_owner: int | None = None
            self._publication_guard_depth = 0
            (
                self._evidence_parent_fd,
                self._evidence_parent_identity,
            ) = self._open_directory_authority(evidence_path.parent)
        except BaseException:
            publication_lock_fd = getattr(self, "_publication_lock_fd", -1)
            if publication_lock_fd >= 0:
                os.close(publication_lock_fd)
                self._publication_lock_fd = -1
            os.close(self._root_fd)
            self._root_fd = -1
            raise
        self._live_bundle_retention = live_bundle_retention
        self._candidate_retention_seconds = candidate_retention_seconds
        self._share_segment_size = share_segment_size
        self._verifier_timeout_seconds = verifier_timeout_seconds
        self._canonicalizer = canonicalizer
        self._verifier = verifier
        self._wall_time = wall_time
        self._lock = threading.RLock()
        self._closed = False
        self._store_token = uuid.uuid4().hex
        self._active_candidates: dict[str, tuple[Path, _FileIdentity | None]] = {}
        self._latest_evidence: dict[str, Any] | None = None
        self._current_envelope: Path | None = None
        self._current_identity: AuditPublicationIdentity | None = None
        self._evidence_state = "absent"
        self._compatibility_evidence_override = False
        self._invalidated_legacy_identity: AuditPublicationIdentity | None = None
        self._legacy_proof_token: _LegacyProofToken | None = None
        self._load_current_evidence()

    @property
    def root(self) -> Path:
        return self._root

    @property
    def evidence_path(self) -> Path:
        return self._evidence_path

    @property
    def share_segment_size(self) -> int:
        return self._share_segment_size

    @property
    def live_bundle_retention(self) -> int:
        return self._live_bundle_retention

    @property
    def candidate_retention_seconds(self) -> int:
        return self._candidate_retention_seconds

    @staticmethod
    def _directory_identity(path: Path) -> tuple[int, int]:
        value = path.lstat()
        if not stat.S_ISDIR(value.st_mode) or stat.S_ISLNK(value.st_mode):
            raise RuntimeError("audit artifact parent must be a non-symlink directory")
        return value.st_dev, value.st_ino

    @staticmethod
    def _open_directory_authority(path: Path) -> tuple[int, tuple[int, int]]:
        fd = os.open(
            path,
            os.O_RDONLY
            | getattr(os, "O_DIRECTORY", 0)
            | getattr(os, "O_NOFOLLOW", 0),
        )
        try:
            value = os.fstat(fd)
            if not stat.S_ISDIR(value.st_mode):
                raise RuntimeError("audit artifact parent is not a directory")
            identity = (value.st_dev, value.st_ino)
            if AuditArtifactStore._directory_identity(path) != identity:
                raise RuntimeError("audit artifact parent identity changed")
            return fd, identity
        except RuntimeError:
            os.close(fd)
            raise
        except OSError as exc:
            os.close(fd)
            raise RuntimeError(
                "audit artifact parent authority is invalid"
            ) from exc
        except BaseException:
            os.close(fd)
            raise

    @staticmethod
    def _open_publication_lock_authority(
        root: Path,
        root_fd: int,
        root_identity: tuple[int, int],
    ) -> tuple[int, _FileIdentity]:
        flags = os.O_RDWR | getattr(os, "O_NOFOLLOW", 0)
        created = False
        try:
            try:
                fd = os.open(
                    _PUBLICATION_LOCK_NAME,
                    flags | os.O_CREAT | os.O_EXCL,
                    0o600,
                    dir_fd=root_fd,
                )
                created = True
            except FileExistsError:
                fd = os.open(_PUBLICATION_LOCK_NAME, flags, dir_fd=root_fd)
        except OSError as exc:
            raise RuntimeError(
                "audit publication lock cannot be opened safely"
            ) from exc
        try:
            value = os.fstat(fd)
            identity = _FileIdentity.from_stat(value)
            if not stat.S_ISREG(value.st_mode):
                raise RuntimeError("audit publication lock is not a regular file")
            linked = os.stat(
                _PUBLICATION_LOCK_NAME,
                dir_fd=root_fd,
                follow_symlinks=False,
            )
            if not identity.matches(linked):
                raise RuntimeError("audit publication lock identity changed")
            root_value = os.fstat(root_fd)
            if (root_value.st_dev, root_value.st_ino) != root_identity:
                raise RuntimeError("audit artifact root authority is invalid")
            if AuditArtifactStore._directory_identity(root) != root_identity:
                raise RuntimeError("audit artifact root identity changed")
            if created:
                os.fsync(fd)
                os.fsync(root_fd)
            return fd, identity
        except RuntimeError:
            os.close(fd)
            raise
        except OSError as exc:
            os.close(fd)
            raise RuntimeError(
                "audit publication lock authority is invalid"
            ) from exc
        except BaseException:
            os.close(fd)
            raise

    def close(self) -> None:
        lock = getattr(self, "_lock", None)
        publication_lock = getattr(self, "_publication_lifecycle_lock", None)
        if lock is None:
            self._close_unlocked()
            return
        if publication_lock is None:
            with lock:
                self._close_unlocked()
            return
        with publication_lock:
            if (
                self._publication_guard_owner == threading.get_ident()
                and self._publication_guard_depth > 0
            ):
                raise RuntimeError(
                    "cannot close audit artifact store inside publication guard"
                )
            with lock:
                self._close_unlocked()

    def _close_unlocked(self) -> None:
        if getattr(self, "_closed", False):
            return
        self._closed = True
        for field in (
            "_publication_lock_fd",
            "_root_fd",
            "_evidence_parent_fd",
        ):
            fd = getattr(self, field, -1)
            if isinstance(fd, int) and fd >= 0:
                try:
                    os.close(fd)
                except OSError:
                    pass
                setattr(self, field, -1)

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass

    def _validate_publication_lock_identity(
        self,
        *,
        root: Path | None = None,
        root_fd: int | None = None,
        root_identity: tuple[int, int] | None = None,
        lock_fd: int | None = None,
        lock_identity: _FileIdentity | None = None,
    ) -> None:
        root = self._root if root is None else root
        root_fd = self._root_fd if root_fd is None else root_fd
        root_identity = (
            self._root_identity if root_identity is None else root_identity
        )
        lock_fd = self._publication_lock_fd if lock_fd is None else lock_fd
        lock_identity = (
            self._publication_lock_identity
            if lock_identity is None
            else lock_identity
        )
        if getattr(self, "_closed", False) or root_fd < 0 or lock_fd < 0:
            raise RuntimeError("audit artifact store is closed")
        try:
            if self._directory_identity(root) != root_identity:
                raise RuntimeError("audit artifact root identity changed")
            root_value = os.fstat(root_fd)
            if (root_value.st_dev, root_value.st_ino) != root_identity:
                raise RuntimeError("audit artifact root authority is invalid")
            value = os.fstat(lock_fd)
            if not lock_identity.matches(value):
                raise RuntimeError("audit publication lock authority is invalid")
            linked = os.stat(
                _PUBLICATION_LOCK_NAME,
                dir_fd=root_fd,
                follow_symlinks=False,
            )
        except OSError as exc:
            raise RuntimeError("audit publication lock identity changed") from exc
        if not lock_identity.matches(linked):
            raise RuntimeError("audit publication lock identity changed")

    @contextmanager
    def publication_order_guard(self) -> Iterator[None]:
        """Serialize ordinal allocation/publication across threads/processes."""

        with self._publication_order_guard(validate_on_exit=True):
            yield

    @contextmanager
    def _publication_order_guard(
        self,
        *,
        validate_on_exit: bool,
    ) -> Iterator[None]:
        thread_id = threading.get_ident()
        initial_identity = self._publication_lock_identity
        _reject_cross_store_local_owner(
            (initial_identity.device, initial_identity.inode),
            thread_id=thread_id,
            store_id=id(self),
        )
        with self._publication_lifecycle_lock:
            stable_identity = self._publication_lock_identity
            _reject_cross_store_local_owner(
                (stable_identity.device, stable_identity.inode),
                thread_id=thread_id,
                store_id=id(self),
            )
            process_lock = self._publication_inode_thread_lock
            with process_lock:
                with self._publication_order_guard_locked(
                    validate_on_exit=validate_on_exit
                ):
                    yield

    @contextmanager
    def _publication_order_guard_locked(
        self,
        *,
        validate_on_exit: bool,
    ) -> Iterator[None]:
        """Guard body with lifecycle and current inode thread locks held."""

        thread_id = threading.get_ident()
        if self._publication_guard_owner == thread_id:
            self._publication_guard_depth += 1
            try:
                self._validate_publication_lock_identity()
                yield
                if validate_on_exit:
                    self._validate_publication_lock_identity()
            finally:
                self._publication_guard_depth -= 1
            return
        if self._publication_guard_owner is not None:
            raise RuntimeError("audit publication guard ownership is inconsistent")
        self._validate_publication_lock_identity()
        lock_fd = self._publication_lock_fd
        root = self._root
        root_fd = self._root_fd
        root_identity = self._root_identity
        lock_identity = self._publication_lock_identity
        local_key = (lock_identity.device, lock_identity.inode)
        with _PUBLICATION_THREAD_LOCKS_GUARD:
            local_owner = _PUBLICATION_LOCAL_OWNERS.get(local_key)
        if local_owner is not None:
            owner_thread, owner_store = local_owner
            if owner_thread == thread_id and owner_store != id(self):
                raise RuntimeError(
                    "audit publication guard is already held by another store "
                    "on this thread"
                )
            raise RuntimeError("audit publication guard local ownership is inconsistent")
        fcntl.flock(lock_fd, fcntl.LOCK_EX)
        registered = False
        try:
            with _PUBLICATION_THREAD_LOCKS_GUARD:
                local_owner = _PUBLICATION_LOCAL_OWNERS.get(local_key)
                if local_owner is not None:
                    raise RuntimeError(
                        "audit publication guard local ownership is inconsistent"
                    )
                _PUBLICATION_LOCAL_OWNERS[local_key] = (thread_id, id(self))
                registered = True
            self._publication_guard_owner = thread_id
            self._publication_guard_depth = 1
            self._validate_publication_lock_identity(
                root=root,
                root_fd=root_fd,
                root_identity=root_identity,
                lock_fd=lock_fd,
                lock_identity=lock_identity,
            )
            yield
            if validate_on_exit:
                self._validate_publication_lock_identity(
                    root=root,
                    root_fd=root_fd,
                    root_identity=root_identity,
                    lock_fd=lock_fd,
                    lock_identity=lock_identity,
                )
        finally:
            self._publication_guard_depth = 0
            self._publication_guard_owner = None
            if registered:
                with _PUBLICATION_THREAD_LOCKS_GUARD:
                    if _PUBLICATION_LOCAL_OWNERS.get(local_key) == (
                        thread_id,
                        id(self),
                    ):
                        del _PUBLICATION_LOCAL_OWNERS[local_key]
            fcntl.flock(lock_fd, fcntl.LOCK_UN)

    def _require_publication_order_guard(self) -> None:
        if (
            self._publication_guard_owner != threading.get_ident()
            or self._publication_guard_depth <= 0
        ):
            raise RuntimeError("audit publication order guard is required")
        self._validate_publication_lock_identity()

    @contextmanager
    def _prepared_publication_order_guard(
        self,
        *,
        root: Path,
        root_fd: int,
        root_identity: tuple[int, int],
        lock_fd: int,
        lock_identity: _FileIdentity,
    ) -> Iterator[None]:
        current_identity = self._publication_lock_identity
        if (
            lock_identity.device == current_identity.device
            and lock_identity.inode == current_identity.inode
        ):
            self._validate_publication_lock_identity(
                root=root,
                root_fd=root_fd,
                root_identity=root_identity,
                lock_fd=lock_fd,
                lock_identity=lock_identity,
            )
            yield
            self._validate_publication_lock_identity(
                root=root,
                root_fd=root_fd,
                root_identity=root_identity,
                lock_fd=lock_fd,
                lock_identity=lock_identity,
            )
            return
        if root_identity == self._root_identity:
            raise RuntimeError(
                "audit publication lock changed within the current root"
            )
        process_lock = _publication_thread_lock(
            (lock_identity.device, lock_identity.inode)
        )
        if not process_lock.acquire(blocking=False):
            raise RuntimeError("new audit publication guard is busy")
        flocked = False
        try:
            try:
                fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise RuntimeError("new audit publication guard is busy") from exc
            flocked = True
            self._validate_publication_lock_identity(
                root=root,
                root_fd=root_fd,
                root_identity=root_identity,
                lock_fd=lock_fd,
                lock_identity=lock_identity,
            )
            yield
            self._validate_publication_lock_identity(
                root=root,
                root_fd=root_fd,
                root_identity=root_identity,
                lock_fd=lock_fd,
                lock_identity=lock_identity,
            )
        finally:
            if flocked:
                fcntl.flock(lock_fd, fcntl.LOCK_UN)
            process_lock.release()

    def _validate_root_identity(self) -> None:
        if getattr(self, "_closed", False) or self._root_fd < 0:
            raise RuntimeError("audit artifact store is closed")
        try:
            current = self._directory_identity(self._root)
        except (FileNotFoundError, OSError) as exc:
            raise RuntimeError("audit artifact root identity changed") from exc
        if current != self._root_identity:
            raise RuntimeError("audit artifact root identity changed")
        value = os.fstat(self._root_fd)
        if (value.st_dev, value.st_ino) != self._root_identity:
            raise RuntimeError("audit artifact root authority is invalid")

    def _validate_evidence_parent_identity(self) -> None:
        if getattr(self, "_closed", False) or self._evidence_parent_fd < 0:
            raise RuntimeError("audit artifact store is closed")
        try:
            current = self._directory_identity(self._evidence_path.parent)
        except (FileNotFoundError, OSError) as exc:
            raise RuntimeError("audit evidence parent identity changed") from exc
        if current != self._evidence_parent_identity:
            raise RuntimeError("audit evidence parent identity changed")
        value = os.fstat(self._evidence_parent_fd)
        if (value.st_dev, value.st_ino) != self._evidence_parent_identity:
            raise RuntimeError("audit evidence parent authority is invalid")

    def _owned_parent_fd(self, path: Path) -> int | None:
        path = Path(path).absolute()
        try:
            parent = path.parent.resolve(strict=True)
        except OSError:
            parent = path.parent
        if parent == self._root:
            return self._root_fd
        if parent == self._evidence_path.parent:
            return self._evidence_parent_fd
        return None

    def duplicate_root_directory_fd(self) -> int:
        with self._lock:
            self._validate_root_identity()
            return os.dup(self._root_fd)

    def _owned_lstat(self, path: Path) -> os.stat_result:
        fd = self._owned_parent_fd(path)
        if fd is None:
            raise RuntimeError("audit stat target has no directory authority")
        return os.stat(Path(path).name, dir_fd=fd, follow_symlinks=False)

    def _owned_open(self, path: Path, flags: int, mode: int = 0o777) -> int:
        fd = self._owned_parent_fd(path)
        if fd is None:
            mutation_flags = (
                os.O_WRONLY
                | os.O_RDWR
                | os.O_CREAT
                | os.O_EXCL
                | getattr(os, "O_TRUNC", 0)
            )
            if flags & mutation_flags:
                raise RuntimeError("audit mutation target has no directory authority")
            raise RuntimeError("audit read target has no directory authority")
        return os.open(Path(path).name, flags, mode, dir_fd=fd)

    def _owned_unlink(self, path: Path) -> None:
        fd = self._owned_parent_fd(path)
        if fd is None:
            raise RuntimeError("audit unlink target has no directory authority")
        os.unlink(Path(path).name, dir_fd=fd)

    def _owned_replace(self, source: Path, target: Path) -> None:
        source_fd = self._owned_parent_fd(source)
        target_fd = self._owned_parent_fd(target)
        if source_fd is None or target_fd is None:
            raise RuntimeError("audit replace target has no directory authority")
        os.replace(
            Path(source).name,
            Path(target).name,
            src_dir_fd=source_fd,
            dst_dir_fd=target_fd,
        )

    def _owned_link(self, source: Path, target: Path) -> None:
        source_fd = self._owned_parent_fd(source)
        target_fd = self._owned_parent_fd(target)
        if source_fd is None or target_fd is None:
            raise RuntimeError("audit link target has no directory authority")
        os.link(
            Path(source).name,
            Path(target).name,
            src_dir_fd=source_fd,
            dst_dir_fd=target_fd,
            follow_symlinks=False,
        )

    def _read_owned_regular_bytes(
        self,
        path: Path,
    ) -> tuple[bytes, os.stat_result]:
        parent_fd = self._owned_parent_fd(path)
        if parent_fd is not None:
            self._validate_owned_parent(path)
        fd = self._owned_open(
            path,
            os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
        )
        result = self._read_regular_bytes_fd(fd)
        if parent_fd is not None:
            self._validate_owned_parent(path)
        return result

    def _validate_owned_parent(self, path: Path) -> None:
        path = Path(path).absolute()
        try:
            parent = path.parent.resolve(strict=True)
        except OSError:
            parent = path.parent
        if parent == self._root:
            self._validate_root_identity()
        elif parent == self._evidence_path.parent:
            self._validate_evidence_parent_identity()
        else:
            raise RuntimeError("audit target has no directory authority")

    def reconfigure(
        self,
        *,
        root: Path | None = None,
        evidence_path: Path | None = None,
        live_bundle_retention: int | None = None,
        candidate_retention_seconds: int | None = None,
        share_segment_size: int | None = None,
        canonicalizer: Callable[[dict[str, Any]], bytes] | None = None,
    ) -> None:
        if (
            self._publication_guard_owner == threading.get_ident()
            and self._publication_guard_depth > 0
        ):
            raise RuntimeError(
                "cannot reconfigure audit artifact store inside publication guard"
            )
        retired_root_fd: int | None = None
        retired_publication_lock_fd: int | None = None
        with self._publication_order_guard(validate_on_exit=False):
            (
                retired_root_fd,
                retired_publication_lock_fd,
            ) = self._reconfigure_under_publication_guard(
                root=root,
                evidence_path=evidence_path,
                live_bundle_retention=live_bundle_retention,
                candidate_retention_seconds=candidate_retention_seconds,
                share_segment_size=share_segment_size,
                canonicalizer=canonicalizer,
            )
        for retired_fd in (retired_publication_lock_fd, retired_root_fd):
            if retired_fd is not None:
                try:
                    os.close(retired_fd)
                except OSError:
                    pass

    def _reconfigure_under_publication_guard(
        self,
        *,
        root: Path | None,
        evidence_path: Path | None,
        live_bundle_retention: int | None,
        candidate_retention_seconds: int | None,
        share_segment_size: int | None,
        canonicalizer: Callable[[dict[str, Any]], bytes] | None,
    ) -> tuple[int | None, int | None]:
        self._require_publication_order_guard()
        with self._lock:
            if self._closed:
                raise RuntimeError("audit artifact store is closed")
            if root is not None and self._active_candidates:
                raise RuntimeError(
                    "cannot reconfigure audit root while candidates are active"
                )
            size = self._share_segment_size
            if share_segment_size is not None:
                size = int(share_segment_size)
                if size < 0:
                    raise ValueError(
                        "audit share segment size must be non-negative"
                    )
            new_root = self._root
            new_root_fd: int | None = None
            new_root_identity = self._root_identity
            new_publication_lock_fd: int | None = None
            new_publication_lock_identity = self._publication_lock_identity
            new_evidence_path = self._evidence_path
            new_evidence_parent_fd: int | None = None
            new_evidence_parent_identity = self._evidence_parent_identity
            try:
                if root is not None:
                    candidate_root = Path(root).expanduser().absolute()
                    candidate_root.mkdir(parents=True, exist_ok=True)
                    new_root = candidate_root.resolve(strict=True)
                    new_root_fd, new_root_identity = (
                        self._open_directory_authority(new_root)
                    )
                    (
                        new_publication_lock_fd,
                        new_publication_lock_identity,
                    ) = self._open_publication_lock_authority(
                        new_root,
                        new_root_fd,
                        new_root_identity,
                    )
                if evidence_path is not None:
                    candidate_evidence = Path(evidence_path).expanduser().absolute()
                    candidate_evidence.parent.mkdir(parents=True, exist_ok=True)
                    new_evidence_path = (
                        candidate_evidence.parent.resolve(strict=True)
                        / candidate_evidence.name
                    )
                    (
                        new_evidence_parent_fd,
                        new_evidence_parent_identity,
                    ) = self._open_directory_authority(
                        new_evidence_path.parent
                    )
            except BaseException:
                for prepared_fd in (
                    new_publication_lock_fd,
                    new_root_fd,
                    new_evidence_parent_fd,
                ):
                    if prepared_fd is not None:
                        os.close(prepared_fd)
                raise
            new_live_bundle_retention = (
                self._live_bundle_retention
                if live_bundle_retention is None
                else int(live_bundle_retention)
            )
            new_candidate_retention_seconds = (
                self._candidate_retention_seconds
                if candidate_retention_seconds is None
                else int(candidate_retention_seconds)
            )
            prepared_guard = (
                self._prepared_publication_order_guard(
                    root=new_root,
                    root_fd=new_root_fd,
                    root_identity=new_root_identity,
                    lock_fd=new_publication_lock_fd,
                    lock_identity=new_publication_lock_identity,
                )
                if new_root_fd is not None
                and new_publication_lock_fd is not None
                else nullcontext()
            )
            old_root = self._root
            old_root_fd = self._root_fd
            old_root_identity = self._root_identity
            old_publication_lock_fd = self._publication_lock_fd
            old_publication_lock_identity = self._publication_lock_identity
            old_publication_inode_thread_lock = (
                self._publication_inode_thread_lock
            )
            old_evidence_path = self._evidence_path
            old_evidence_parent_fd = self._evidence_parent_fd
            old_evidence_parent_identity = self._evidence_parent_identity
            old_latest_evidence = self._latest_evidence
            old_current_envelope = self._current_envelope
            old_current_identity = self._current_identity
            old_evidence_state = self._evidence_state
            old_compatibility_override = self._compatibility_evidence_override
            old_invalidated_legacy_identity = self._invalidated_legacy_identity
            old_legacy_proof_token = self._legacy_proof_token
            old_live_retention = self._live_bundle_retention
            old_candidate_retention = self._candidate_retention_seconds
            old_share_segment_size = self._share_segment_size
            old_canonicalizer = self._canonicalizer
            try:
                with prepared_guard:
                    # Final old-authority validation is the linearization
                    # boundary. After this succeeds, only the prepared new
                    # authority may influence commit or rollback.
                    self._validate_publication_lock_identity(
                        root=old_root,
                        root_fd=old_root_fd,
                        root_identity=old_root_identity,
                        lock_fd=old_publication_lock_fd,
                        lock_identity=old_publication_lock_identity,
                    )
                    if new_root_fd is not None:
                        self._root = new_root
                        self._root_fd = new_root_fd
                        self._root_identity = new_root_identity
                        assert new_publication_lock_fd is not None
                        self._publication_lock_fd = new_publication_lock_fd
                        self._publication_lock_identity = (
                            new_publication_lock_identity
                        )
                        self._publication_inode_thread_lock = _publication_thread_lock(
                            (
                                new_publication_lock_identity.device,
                                new_publication_lock_identity.inode,
                            )
                        )
                    if new_evidence_parent_fd is not None:
                        self._evidence_path = new_evidence_path
                        self._evidence_parent_fd = new_evidence_parent_fd
                        self._evidence_parent_identity = (
                            new_evidence_parent_identity
                        )
                    if root is not None or evidence_path is not None:
                        self._latest_evidence = None
                        self._current_envelope = None
                        self._current_identity = None
                        self._evidence_state = "absent"
                        self._compatibility_evidence_override = False
                        self._invalidated_legacy_identity = None
                        self._legacy_proof_token = None
                    self._live_bundle_retention = new_live_bundle_retention
                    self._candidate_retention_seconds = (
                        new_candidate_retention_seconds
                    )
                    self._share_segment_size = size
                    if canonicalizer is not None:
                        self._canonicalizer = canonicalizer
                    if root is not None or evidence_path is not None:
                        # Readers must see either the complete old authority/cache
                        # or the complete new authority/cache. Both old and new
                        # process guards remain held across switch and reload.
                        self._reload_current_evidence_locked()
                if new_evidence_parent_fd is not None:
                    try:
                        os.close(old_evidence_parent_fd)
                    except OSError:
                        pass
                return (
                    old_root_fd if new_root_fd is not None else None,
                    (
                        old_publication_lock_fd
                        if new_publication_lock_fd is not None
                        else None
                    ),
                )
            except BaseException:
                self._root = old_root
                self._root_fd = old_root_fd
                self._root_identity = old_root_identity
                self._publication_lock_fd = old_publication_lock_fd
                self._publication_lock_identity = old_publication_lock_identity
                self._publication_inode_thread_lock = (
                    old_publication_inode_thread_lock
                )
                self._evidence_path = old_evidence_path
                self._evidence_parent_fd = old_evidence_parent_fd
                self._evidence_parent_identity = old_evidence_parent_identity
                self._latest_evidence = old_latest_evidence
                self._current_envelope = old_current_envelope
                self._current_identity = old_current_identity
                self._evidence_state = old_evidence_state
                self._compatibility_evidence_override = (
                    old_compatibility_override
                )
                self._invalidated_legacy_identity = (
                    old_invalidated_legacy_identity
                )
                self._legacy_proof_token = old_legacy_proof_token
                self._live_bundle_retention = old_live_retention
                self._candidate_retention_seconds = old_candidate_retention
                self._share_segment_size = old_share_segment_size
                self._canonicalizer = old_canonicalizer
                for prepared_fd in (
                    new_publication_lock_fd,
                    new_root_fd,
                    new_evidence_parent_fd,
                ):
                    if prepared_fd is not None:
                        try:
                            os.close(prepared_fd)
                        except OSError:
                            pass
                raise

    def publication_sequence_floor(self) -> int:
        with self._lock:
            if not self._directory_authority_is_current():
                return 0
            return (
                self._current_identity.sequence
                if self._current_identity is not None
                and self._evidence_state in {"valid", "legacy_proven"}
                else 0
            )

    def legacy_evidence_identity(self) -> AuditPublicationIdentity | None:
        with self._lock:
            if not self._directory_authority_is_current():
                return None
            if (
                self._publication_guard_owner == threading.get_ident()
                and self._publication_guard_depth > 0
                and not self._compatibility_evidence_override
            ):
                self._reload_current_evidence_locked()
            if self._evidence_state not in {
                "legacy",
                "legacy_unproven",
            } or self._current_identity is None:
                return None
            return self._current_identity

    def _directory_authority_is_current(self) -> bool:
        try:
            self._validate_root_identity()
            self._validate_evidence_parent_identity()
            return True
        except RuntimeError:
            # A transient pathname replacement revokes authority while it is
            # present, but the pinned fd/cache can safely recover if the exact
            # original directory inode is restored. Malformed evidence uses
            # the separate sticky `invalid` state.
            return False

    @staticmethod
    def artifact_kind(name: str) -> str:
        if _BODY_RE.fullmatch(name):
            return "body"
        segment = _SHARE_CONTENT_RE.fullmatch(name) or _SHARE_SLOT_RE.fullmatch(name)
        if segment:
            return (
                "share_segment"
                if int(segment.group("first")) <= int(segment.group("last"))
                else "other"
            )
        if (
            _CANDIDATE_RE.fullmatch(name)
            or _LEGACY_CANDIDATE_RE.fullmatch(name)
            or _LEGACY_HIDDEN_CANDIDATE_RE.fullmatch(name)
        ):
            return "candidate"
        if _LIVE_RE.fullmatch(name):
            return "live_bundle"
        return "other"

    def metrics_snapshot(self) -> dict[str, dict[str, int] | int]:
        metrics: dict[str, dict[str, int] | int] = {
            kind: {"files": 0, "bytes": 0}
            for kind in (
                "body",
                "share_segment",
                "live_bundle",
                "candidate",
                "other",
            )
        }
        metrics["scan_error"] = 0
        try:
            self._validate_root_identity()
            paths = [self._root / name for name in os.listdir(self._root_fd)]
        except (OSError, RuntimeError):
            metrics["scan_error"] = 1
            return metrics
        for path in paths:
            if path.name == _PUBLICATION_LOCK_NAME:
                continue
            try:
                value = self._owned_lstat(path)
                if not stat.S_ISREG(value.st_mode):
                    continue
            except RuntimeError:
                metrics["scan_error"] = 1
                break
            except OSError:
                metrics["scan_error"] = 1
                continue
            kind = self.artifact_kind(path.name)
            bucket = metrics[kind]
            assert isinstance(bucket, dict)
            bucket["files"] += 1
            bucket["bytes"] += value.st_size
        try:
            self._validate_root_identity()
        except RuntimeError:
            metrics["scan_error"] = 1
        return metrics

    def issue_candidate(self, *, block_hash: str) -> OwnedCandidateArtifact:
        block_hash = _canonical_hex(block_hash, name="block_hash")
        with self._lock:
            self._validate_root_identity()
            for _attempt in range(16):
                token = uuid.uuid4().hex
                path = self._root / (
                    f".prism-live-audit-bundle-candidate-{block_hash}-{token}.json.tmp"
                )
                try:
                    self._owned_lstat(path)
                except FileNotFoundError:
                    candidate = OwnedCandidateArtifact(
                        path=path,
                        block_hash=block_hash,
                        token=token,
                        store_token=self._store_token,
                    )
                    self._active_candidates[token] = (path, None)
                    return candidate
            raise RuntimeError("could not allocate an absent audit candidate path")

    def _require_candidate(
        self,
        candidate: OwnedCandidateArtifact,
    ) -> tuple[Path, _FileIdentity | None]:
        self._validate_root_identity()
        if candidate.store_token != self._store_token:
            raise RuntimeError("candidate does not belong to this audit store")
        if not _CANDIDATE_RE.fullmatch(candidate.path.name):
            raise RuntimeError("candidate has an invalid owned filename")
        if candidate.path.parent != self._root:
            raise RuntimeError("candidate escapes the audit artifact root")
        current = self._active_candidates.get(candidate.token)
        if current is None or current[0] != candidate.path:
            raise RuntimeError("candidate is no longer active")
        return current

    def adopt_created_candidate(
        self,
        candidate: OwnedCandidateArtifact,
    ) -> None:
        raise RuntimeError(
            "pathname adoption is forbidden; transfer the exact open compiler inode"
        )

    def adopt_compiler_candidate(
        self,
        candidate: OwnedCandidateArtifact,
        *,
        path: Path,
        value: os.stat_result,
    ) -> None:
        """Transfer the exact still-open inode created exclusively by J1."""

        with self._lock:
            expected_path, identity = self._require_candidate(candidate)
            if identity is not None or Path(path) != expected_path:
                raise RuntimeError("compiler candidate transfer is invalid")
            transferred = _FileIdentity.from_stat(value)
            current = self._owned_lstat(expected_path)
            if not transferred.matches(current):
                raise RuntimeError("compiler candidate identity changed before transfer")
            self._active_candidates[candidate.token] = (expected_path, transferred)

    def write_compatibility_candidate(
        self,
        candidate: OwnedCandidateArtifact,
        bundle: Mapping[str, Any],
    ) -> Path:
        payload = _json_bytes(bundle)
        with self._lock:
            path, identity = self._require_candidate(candidate)
            if identity is not None:
                raise RuntimeError("candidate already exists")
        created = False
        try:
            fd = self._owned_open(
                path,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
                0o600,
            )
            created = True
            opened = os.fstat(fd)
            if not stat.S_ISREG(opened.st_mode):
                raise RuntimeError("candidate is not a regular file")
            candidate_identity = _FileIdentity.from_stat(opened)
            with self._lock:
                self._require_candidate(candidate)
                self._active_candidates[candidate.token] = (
                    path,
                    candidate_identity,
                )
            with os.fdopen(fd, "wb") as handle:
                try:
                    handle.write(payload)
                    handle.flush()
                    os.fsync(handle.fileno())
                finally:
                    finalized_identity = _FileIdentity.from_stat(
                        os.fstat(handle.fileno())
                    )
                    with self._lock:
                        current = self._active_candidates.get(candidate.token)
                        if current == (path, candidate_identity):
                            self._active_candidates[candidate.token] = (
                                path,
                                finalized_identity,
                            )
            self._validate_root_identity()
            return path
        except BaseException:
            if created:
                self._unlink_candidate_if_same(candidate, allow_unadopted=True)
            else:
                self.release_candidate(candidate)
            raise

    def release_candidate(self, candidate: OwnedCandidateArtifact) -> None:
        """Release an uncreated reservation without acquiring delete authority."""

        with self._lock:
            self._require_candidate(candidate)
            self._active_candidates.pop(candidate.token, None)

    def _unlink_candidate_if_same(
        self,
        candidate: OwnedCandidateArtifact,
        *,
        allow_unadopted: bool = False,
    ) -> None:
        with self._lock:
            self._validate_root_identity()
            current = self._active_candidates.get(candidate.token)
            if current is None or current[0] != candidate.path:
                return
            expected = current[1]
            try:
                value = self._owned_lstat(candidate.path)
            except FileNotFoundError:
                self._active_candidates.pop(candidate.token, None)
                return
            if not stat.S_ISREG(value.st_mode):
                self._active_candidates.pop(candidate.token, None)
                return
            if expected is not None and not expected.matches(value):
                self._active_candidates.pop(candidate.token, None)
                return
            if expected is None and not allow_unadopted:
                return
            self._remove_identity_safe(
                candidate.path,
                _FileIdentity.from_stat(value),
            )
            self._active_candidates.pop(candidate.token, None)

    def discard_candidate(self, candidate: OwnedCandidateArtifact) -> None:
        with self._lock:
            current = self._active_candidates.get(candidate.token)
            if current is not None and current[1] is None:
                # Reservation alone never grants deletion authority. J1 must
                # return and transfer the exact created inode first.
                self._active_candidates.pop(candidate.token, None)
                return
        self._unlink_candidate_if_same(candidate)

    @staticmethod
    def verified_canonical_bundle_path(
        candidate_bundle_path: Path,
        report: Mapping[str, Any],
    ) -> Path | None:
        expected = str(report["audit_bundle_sha256_hex"]).lower()
        payload, _value = AuditArtifactStore.read_regular_bytes(
            Path(candidate_bundle_path)
        )
        return (
            Path(candidate_bundle_path)
            if _sha256_bytes(payload) == expected
            else None
        )

    def verify_candidate(
        self,
        candidate: OwnedCandidateArtifact,
        *,
        coinbase_tx_hex: str,
        expected_coinbase_value_sats: int,
        trusted_writer_public_key_hex: str,
        trust_source: str = "configured",
        expected_block_height: int | None = None,
        verifier: Callable[..., dict[str, Any]] | None = None,
    ) -> VerifiedAuditBundle:
        key = _canonical_hex(
            trusted_writer_public_key_hex,
            name="ledger writer public key",
        )
        coinbase_tx_hex = _canonical_hex_bytes(
            coinbase_tx_hex,
            name="coinbase_tx_hex",
        )
        with self._lock:
            path, identity = self._require_candidate(candidate)
            if identity is None:
                raise RuntimeError(
                    "candidate compiler inode was not transferred before verification"
                )
        before = self._owned_lstat(path)
        if not identity.matches(before):
            raise RuntimeError("candidate identity changed before verification")
        before_bytes, before_descriptor_stat = self._read_owned_regular_bytes(path)
        if not identity.matches(before_descriptor_stat):
            raise RuntimeError("candidate identity changed before verification")
        literal_before = _sha256_bytes(before_bytes)
        verify = verifier or self._verifier or self.verify_bundle
        snapshot_fd, verification_path = self._open_verification_snapshot(
            before_bytes
        )
        try:
            report = verify(
                verification_path,
                coinbase_tx_hex,
                key,
                expected_coinbase_value_sats=expected_coinbase_value_sats,
                expected_block_height=expected_block_height,
            )
        finally:
            os.close(snapshot_fd)
        after_bytes, after = self._read_owned_regular_bytes(path)
        literal_after = _sha256_bytes(after_bytes)
        if (
            not identity.matches(after)
            or before.st_size != after.st_size
            or before.st_mtime_ns != after.st_mtime_ns
            or not hmac.compare_digest(literal_before, literal_after)
        ):
            raise RuntimeError("candidate changed during verification")
        normalized = self._validate_verifier_report(
            report,
            coinbase_tx_hex=coinbase_tx_hex,
            expected_coinbase_value_sats=expected_coinbase_value_sats,
            expected_block_height=expected_block_height,
        )
        expected_digest = str(normalized["audit_bundle_sha256_hex"])
        verification_identity = self.build_verification_identity(
            trust_source=trust_source,
            trusted_writer_public_key_hex=key,
            literal_sha256=literal_after,
            literal_byte_len=after.st_size,
            report=normalized,
        )
        verified = VerifiedAuditBundle(
            candidate=candidate,
            report=copy.deepcopy(normalized),
            literal_sha256=literal_after,
            byte_length=after.st_size,
            device=after.st_dev,
            inode=after.st_ino,
            mtime_ns=after.st_mtime_ns,
            canonical_copy_eligible=hmac.compare_digest(
                literal_after,
                expected_digest,
            ),
            verification_identity=verification_identity,
        )
        self._validate_root_identity()
        return verified

    def require_current_verified_candidate(
        self,
        verified: VerifiedAuditBundle,
        candidate: OwnedCandidateArtifact,
    ) -> None:
        """Reject reuse of a successful result across ephemeral candidates."""

        with self._lock:
            path, identity = self._require_candidate(candidate)
            if verified.candidate != candidate or identity is None:
                raise RuntimeError("verified audit result belongs to another candidate")
            if (
                identity.device != verified.device
                or identity.inode != verified.inode
                or path != candidate.path
            ):
                raise RuntimeError("verified audit result candidate identity changed")

    @staticmethod
    def build_verification_identity(
        *,
        trust_source: str,
        trusted_writer_public_key_hex: str,
        literal_sha256: str,
        literal_byte_len: int,
        report: Mapping[str, Any],
    ) -> dict[str, Any]:
        if trust_source not in {"configured", "embedded_test_only"}:
            raise RuntimeError("audit verifier trust source is invalid")
        normalized_report = AuditArtifactStore._normalize_report_identity(report)
        byte_len = literal_byte_len
        if isinstance(byte_len, bool) or not isinstance(byte_len, int) or byte_len < 0:
            raise RuntimeError("audit verifier literal byte length is invalid")
        base = {
            "schema": VERIFICATION_IDENTITY_SCHEMA,
            "trust_source": trust_source,
            "ledger_writer_public_key_hex": _canonical_hex(
                trusted_writer_public_key_hex,
                name="ledger writer public key",
            ),
            "literal_sha256_hex": _canonical_hex(
                literal_sha256,
                name="verified literal sha256",
            ),
            "literal_byte_len": byte_len,
            "report": normalized_report,
        }
        return {
            **base,
            "identity_sha256_hex": _sha256_bytes(
                _json_bytes(base, sort_keys=True)
            ),
        }

    @staticmethod
    def _normalize_verification_identity(
        value: object,
        *,
        report: Mapping[str, Any],
    ) -> dict[str, Any]:
        if not isinstance(value, dict):
            raise RuntimeError("audit verification identity is required")
        normalized = AuditArtifactStore.build_verification_identity(
            trust_source=str(value.get("trust_source") or ""),
            trusted_writer_public_key_hex=str(
                value.get("ledger_writer_public_key_hex") or ""
            ),
            literal_sha256=str(value.get("literal_sha256_hex") or ""),
            literal_byte_len=value.get("literal_byte_len"),
            report=report,
        )
        if value != normalized:
            raise RuntimeError("audit verification identity mismatch")
        return normalized

    @staticmethod
    def _legacy_verification_marker(
        *,
        identity: AuditPublicationIdentity,
        report: Mapping[str, Any],
    ) -> dict[str, Any]:
        normalized_report = AuditArtifactStore._normalize_report_identity(report)
        return {
            "schema": LEGACY_VERIFICATION_UNAVAILABLE_SCHEMA,
            "reason": "evidence-predates-deterministic-verification-identity",
            "block_hash": identity.block_hash,
            "block_height": identity.block_height,
            "audit_publication_sequence": identity.sequence,
            "report_identity_sha256_hex": _sha256_bytes(
                _json_bytes(normalized_report, sort_keys=True)
            ),
        }

    @staticmethod
    def _normalize_legacy_verification_marker(
        value: object,
        *,
        identity: AuditPublicationIdentity,
        report: Mapping[str, Any],
    ) -> dict[str, Any]:
        if not isinstance(value, dict):
            raise RuntimeError("legacy audit verification marker is required")
        expected = AuditArtifactStore._legacy_verification_marker(
            identity=identity,
            report=report,
        )
        if value != expected:
            raise RuntimeError("legacy audit verification marker mismatch")
        return expected

    @staticmethod
    def _loaded_evidence_state(
        payload: Mapping[str, Any],
        identity: AuditPublicationIdentity,
    ) -> str:
        if identity.sequence == 0:
            return "legacy"
        verification = payload.get("audit_verification_identity")
        if (
            isinstance(verification, dict)
            and verification.get("schema")
            == LEGACY_VERIFICATION_UNAVAILABLE_SCHEMA
        ):
            # The on-disk ledger proof is a hint only.  The coordinator must
            # revalidate exact active ledger state on every process start.
            return "legacy_unproven"
        return "valid"

    def _open_verification_snapshot(self, payload: bytes) -> tuple[int, Path]:
        path = self._root / f".prism-audit-verification-{uuid.uuid4().hex}.tmp"
        identity: _FileIdentity | None = None
        read_fd: int | None = None
        try:
            fd = self._owned_open(
                path,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
                0o600,
            )
            identity = _FileIdentity.from_stat(os.fstat(fd))
            with os.fdopen(fd, "wb") as handle:
                try:
                    handle.write(payload)
                    handle.flush()
                    os.fsync(handle.fileno())
                finally:
                    identity = _FileIdentity.from_stat(os.fstat(handle.fileno()))
            read_fd = self._owned_open(
                path,
                os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
            )
            if not identity.matches(os.fstat(read_fd)):
                raise RuntimeError("verification snapshot identity changed")
            if not self._remove_identity_safe(path, identity):
                raise RuntimeError("verification snapshot path was replaced")
            self._fsync_directory(self._root)
            return read_fd, Path(f"/dev/fd/{read_fd}")
        except BaseException:
            if read_fd is not None:
                os.close(read_fd)
            if identity is not None:
                self._remove_identity_safe(path, identity)
            raise

    @staticmethod
    def _validate_verifier_report(
        report: object,
        *,
        coinbase_tx_hex: str,
        expected_coinbase_value_sats: int,
        expected_block_height: int | None,
    ) -> dict[str, Any]:
        if not isinstance(report, dict):
            raise RuntimeError("audit verifier returned a non-object report")
        result = AuditArtifactStore._normalize_report_identity(report)
        if not hmac.compare_digest(result["coinbase_tx_hex"], coinbase_tx_hex):
            raise RuntimeError("audit verifier coinbase does not match submitted coinbase")
        if int(result["coinbase_value_sats"]) != int(expected_coinbase_value_sats):
            raise RuntimeError("audit verifier coinbase value does not match expected value")
        if (
            expected_block_height is not None
            and result["block_height"] != int(expected_block_height)
        ):
            raise RuntimeError("audit verifier block height does not match expected height")
        return result

    @staticmethod
    def _normalize_report_identity(report: Mapping[str, Any]) -> dict[str, Any]:
        result = dict(report)
        if result.get("schema") != VERIFICATION_REPORT_SCHEMA:
            raise RuntimeError("audit verifier report schema is invalid")
        block_height = result.get("block_height")
        if isinstance(block_height, bool) or not isinstance(block_height, int):
            raise RuntimeError("audit verifier block height is invalid")
        if block_height < 0:
            raise RuntimeError("audit verifier block height is invalid")
        result["audit_bundle_sha256_hex"] = _canonical_hex(
            result.get("audit_bundle_sha256_hex"),
            name="audit bundle sha256",
        )
        result["coinbase_tx_hex"] = _canonical_hex_bytes(
            result.get("coinbase_tx_hex"),
            name="report coinbase_tx_hex",
        )
        report_value = result.get("coinbase_value_sats")
        if isinstance(report_value, bool) or not isinstance(report_value, int):
            raise RuntimeError("audit verifier coinbase value is required")
        if result["coinbase_value_sats"] < 0:
            raise RuntimeError("audit verifier coinbase value is invalid")
        for key in (
            "reward_manifest_sha256_hex",
            "payout_policy_manifest_sha256_hex",
            "prism_audit_commitment_leaf_hex",
            "audit_commitment_root_hex",
            "coinbase_txid",
            "coinbase_wtxid",
            "coinbase_manifest_sha256_hex",
        ):
            result[key] = _canonical_hex(result.get(key), name=key)
        for key in (
            "min_output_sats",
            "onchain_output_count",
            "accrued_account_count",
        ):
            value = result.get(key)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise RuntimeError(f"audit verifier {key} is invalid")
        return result

    def verify_bundle(
        self,
        bundle_path: Path,
        coinbase_tx_hex: str,
        ledger_writer_public_key_hex: str,
        *,
        expected_coinbase_value_sats: int,
        expected_block_height: int | None = None,
    ) -> dict[str, Any]:
        del expected_block_height
        inherited_fds: tuple[int, ...] = ()
        match = re.fullmatch(r"/dev/fd/(?P<fd>[0-9]+)", str(bundle_path))
        if match is not None:
            inherited_fds = (int(match.group("fd")),)
        process = subprocess.Popen(
            prism_tool_command("qbit-prism-audit-verify")
            + [
                str(bundle_path),
                "--coinbase-tx-hex",
                coinbase_tx_hex,
                "--ledger-writer-public-key-hex",
                ledger_writer_public_key_hex,
                "--expected-coinbase-value-sats",
                str(expected_coinbase_value_sats),
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            pass_fds=inherited_fds,
            start_new_session=True,
        )
        outputs = {"stdout": bytearray(), "stderr": bytearray()}
        selector = selectors.DefaultSelector()
        assert process.stdout is not None
        assert process.stderr is not None
        for name, stream in (
            ("stdout", process.stdout),
            ("stderr", process.stderr),
        ):
            os.set_blocking(stream.fileno(), False)
            selector.register(stream, selectors.EVENT_READ, name)
        deadline = time.monotonic() + self._verifier_timeout_seconds
        try:
            while selector.get_map() or process.poll() is None:
                remaining_seconds = deadline - time.monotonic()
                if remaining_seconds <= 0:
                    self._kill_verifier_group(process)
                    process.wait()
                    raise RuntimeError("qbit-prism-audit-verify timed out")
                events = selector.select(min(0.05, remaining_seconds))
                for key, _mask in events:
                    try:
                        chunk = os.read(key.fileobj.fileno(), 64 * 1024)
                    except (BlockingIOError, InterruptedError):
                        continue
                    if not chunk:
                        selector.unregister(key.fileobj)
                        continue
                    output = outputs[str(key.data)]
                    remaining = MAX_VERIFIER_OUTPUT_BYTES + 1 - len(output)
                    if remaining > 0:
                        output.extend(chunk[:remaining])
                    if len(output) > MAX_VERIFIER_OUTPUT_BYTES:
                        self._kill_verifier_group(process)
                        process.wait()
                        raise RuntimeError(
                            "qbit-prism-audit-verify output exceeded limit"
                        )
            process.wait()
        except BaseException:
            if process.poll() is None:
                self._kill_verifier_group(process)
                process.wait()
            raise
        finally:
            selector.close()
            process.stdout.close()
            process.stderr.close()
        stdout = bytes(outputs["stdout"])
        stderr = bytes(outputs["stderr"])
        if process.returncode != 0:
            raise RuntimeError(
                "qbit-prism-audit-verify failed: "
                + stderr.decode(errors="replace").strip()
            )
        try:
            report = json.loads(stdout)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise RuntimeError("qbit-prism-audit-verify returned invalid JSON") from exc
        if not isinstance(report, dict):
            raise RuntimeError("qbit-prism-audit-verify returned a non-object report")
        return report

    @staticmethod
    def _kill_verifier_group(process: subprocess.Popen[bytes]) -> None:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except (ProcessLookupError, PermissionError, AttributeError):
            try:
                process.kill()
            except (ProcessLookupError, PermissionError):
                pass

    @staticmethod
    def trusted_writer_key(
        configured_key: str | None,
        bundle: Mapping[str, Any],
        *,
        allow_embedded_test_key: bool,
    ) -> str:
        if configured_key is not None:
            return _canonical_hex(
                configured_key,
                name="configured ledger writer public key",
            )
        if not allow_embedded_test_key:
            raise RuntimeError("configured ledger writer public key is required")
        try:
            embedded = bundle["ledger_window_attestation"]["signature"][
                "public_key_hex"
            ]
        except (KeyError, TypeError) as exc:
            raise RuntimeError("audit bundle has no embedded test writer key") from exc
        return _canonical_hex(embedded, name="bundle ledger public key")

    def live_envelope_path(self, *, block_height: int, block_hash: str) -> Path:
        block_hash = _canonical_hex(block_hash, name="block_hash")
        if isinstance(block_height, bool) or not isinstance(block_height, int):
            raise ValueError("block_height must be an integer")
        if block_height < 0:
            raise ValueError("block_height must be non-negative")
        return self._root / (
            f"prism-live-audit-bundle-{block_height}-{block_hash}.json"
        )

    def body_path(self, block_hash: str, audit_bundle_sha256: str) -> Path:
        block_hash = _canonical_hex(block_hash, name="block_hash")
        digest = _canonical_hex(
            audit_bundle_sha256,
            name="audit_bundle_sha256",
        )
        return self._root / f"prism-audit-bundle-body-{block_hash}-{digest}.json"

    def resolve_owned_path(self, body_uri: object) -> Path:
        self._validate_root_identity()
        raw = Path(str(body_uri)).expanduser()
        if not raw.is_absolute() and (
            len(raw.parts) != 1 or raw.name in {".", ".."}
        ):
            raise RuntimeError(
                f"audit bundle body path escapes audit body store: {body_uri}"
            )
        path = raw if raw.is_absolute() else self._root / raw
        path = path.absolute()
        try:
            path = path.parent.resolve(strict=True) / path.name
        except OSError as exc:
            raise RuntimeError(
                f"audit bundle body path is not resolvable: {body_uri}"
            ) from exc
        if path.parent != self._root:
            raise RuntimeError(
                f"audit bundle body path escapes audit body store: {body_uri}"
            )
        if self.artifact_kind(path.name) not in {"body", "share_segment"}:
            raise RuntimeError(f"audit bundle body path is not owned: {body_uri}")
        self._validate_root_identity()
        return path

    def publish_success(
        self,
        *,
        identity: AuditPublicationIdentity,
        publication_floor_sequence: int,
        report: Mapping[str, Any],
        persistence: Mapping[str, Any],
        evidence: Mapping[str, Any],
        verification_identity: Mapping[str, Any],
        created_at: str,
    ) -> AuditPublication:
        self._require_publication_order_guard()
        block_hash = _canonical_hex(identity.block_hash, name="block_hash")
        if block_hash != identity.block_hash:
            raise ValueError("publication identity block_hash must be canonical")
        if identity.sequence <= 0 or identity.block_height < 0:
            raise ValueError("publication identity is invalid")
        if (
            isinstance(publication_floor_sequence, bool)
            or not isinstance(publication_floor_sequence, int)
            or publication_floor_sequence < 0
        ):
            raise ValueError("publication floor sequence must be a non-negative integer")
        if identity.sequence > publication_floor_sequence:
            raise RuntimeError(
                "audit publication sequence exceeds the durable ledger floor"
            )
        envelope_path = self.live_envelope_path(
            block_height=identity.block_height,
            block_hash=block_hash,
        )
        with self._lock:
            self._validate_root_identity()
            self._validate_evidence_parent_identity()
            if not self._compatibility_evidence_override:
                invalidated_legacy = self._invalidated_legacy_identity
                self._reload_current_evidence_locked()
                if (
                    invalidated_legacy is not None
                    and self._evidence_state in {"legacy", "legacy_unproven"}
                    and self._current_identity == invalidated_legacy
                ):
                    # Explicit invalidation remains repairable only while disk
                    # still names the same unproven legacy publication. A peer
                    # valid publication always wins this reconciliation.
                    self._latest_evidence = None
                    self._current_envelope = None
                    self._current_identity = None
                    self._evidence_state = "invalid"
                else:
                    self._invalidated_legacy_identity = None
            current = self._current_identity
            if self._evidence_state in {"legacy", "legacy_unproven"}:
                raise RuntimeError(
                    "current audit evidence lacks a validated publication identity"
                )
            if current is not None:
                exact = current == identity
                if (
                    self._evidence_state == "legacy_proven"
                    and identity.sequence <= current.sequence
                ):
                    raise RuntimeError(
                        "legacy audit evidence is never exact-replay-equivalent"
                    )
                if identity.sequence == current.sequence and not exact:
                    raise RuntimeError("audit publication identity conflict")
                if identity.sequence < current.sequence:
                    return AuditPublication(
                        identity=identity,
                        envelope_path=envelope_path,
                        evidence=copy.deepcopy(dict(evidence)),
                        published=False,
                    )
                if exact and self._latest_evidence is not None:
                    expected_envelope = self._build_live_envelope(
                        identity=identity,
                        report=report,
                        persistence=persistence,
                        created_at=created_at,
                    )
                    replay_annotations = copy.deepcopy(dict(evidence))
                    # These global counters are observational annotations, not
                    # immutable block identity.  Reuse the originally durable
                    # values so an outbox replay after more shares arrive does
                    # not strand an otherwise exact publication.
                    for annotation in (
                        "accepted_share_count",
                        "distinct_miner_count",
                    ):
                        if annotation in self._latest_evidence:
                            replay_annotations[annotation] = (
                                self._latest_evidence[annotation]
                            )
                    replay_persistence = copy.deepcopy(dict(persistence))
                    current_persistence = self._latest_evidence.get(
                        "persistence"
                    )
                    if isinstance(current_persistence, dict) and (
                        "share_count" in current_persistence
                    ):
                        # persist_accepted_block reports the current global
                        # accepted-share count. It is observational and may
                        # advance between a committed block and outbox replay.
                        replay_persistence["share_count"] = (
                            current_persistence["share_count"]
                        )
                    replay_evidence = self._normalized_durable_evidence(
                        identity=identity,
                        envelope_path=envelope_path,
                        report=report,
                        persistence=replay_persistence,
                        evidence=replay_annotations,
                        verification_identity=verification_identity,
                    )
                    if self._latest_evidence != replay_evidence:
                        raise RuntimeError(
                            "exact audit publication replay payload conflict"
                        )
                    envelope_present = True
                    evidence_present = True
                    try:
                        disk_envelope_bytes, _value = (
                            self._read_owned_regular_bytes(envelope_path)
                        )
                    except FileNotFoundError:
                        envelope_present = False
                    else:
                        try:
                            disk_envelope = json.loads(disk_envelope_bytes)
                        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                            raise RuntimeError(
                                "existing live audit envelope is invalid"
                            ) from exc
                        if not isinstance(disk_envelope, dict):
                            raise RuntimeError(
                                "existing live audit envelope is invalid"
                            )
                        if {
                            key: value
                            for key, value in disk_envelope.items()
                            if key != "created_at"
                        } != {
                            key: value
                            for key, value in expected_envelope.items()
                            if key != "created_at"
                        }:
                            raise RuntimeError(
                                "existing live audit envelope conflicts"
                            )
                    try:
                        disk_evidence_bytes, _value = (
                            self._read_owned_regular_bytes(self._evidence_path)
                        )
                    except FileNotFoundError:
                        evidence_present = False
                    else:
                        try:
                            disk_evidence = json.loads(disk_evidence_bytes)
                        except (UnicodeDecodeError, json.JSONDecodeError):
                            # The evidence path is mutable publication state,
                            # not operator-owned preservation authority.  A
                            # ledger-proven exact replay may repair malformed
                            # bytes while retaining the exact immutable
                            # envelope checked above.
                            evidence_present = False
                        else:
                            if disk_evidence != replay_evidence:
                                try:
                                    self._validate_evidence(disk_evidence)
                                except (
                                    OSError,
                                    RuntimeError,
                                    TypeError,
                                    ValueError,
                                ):
                                    evidence_present = False
                                else:
                                    raise RuntimeError(
                                        "existing live audit evidence conflicts"
                                    )
                    if envelope_present and evidence_present:
                        self._fsync_directory(envelope_path.parent)
                        self._fsync_directory(self._evidence_path.parent)
                        return AuditPublication(
                            identity=identity,
                            envelope_path=envelope_path,
                            evidence=copy.deepcopy(self._latest_evidence),
                            published=False,
                        )
                    if identity.sequence != publication_floor_sequence:
                        raise RuntimeError(
                            "audit publication repair is behind the durable ledger floor"
                        )
                    # A missing exact entry is repaired by the same atomic
                    # primitives as a first publication.  Preserve the durable
                    # observational annotations while falling through.
                    persistence = replay_persistence
                    evidence = replay_annotations
            if identity.sequence != publication_floor_sequence:
                raise RuntimeError(
                    "audit publication is behind the durable ledger floor"
                )
            envelope = self._build_live_envelope(
                identity=identity,
                report=report,
                persistence=persistence,
                created_at=created_at,
            )
            try:
                old_envelope, _old_envelope_stat = self._read_owned_regular_bytes(
                    envelope_path
                )
            except FileNotFoundError:
                old_envelope = None
            reuse_envelope = False
            if old_envelope is not None:
                try:
                    existing_envelope = json.loads(old_envelope)
                except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                    raise RuntimeError(
                        "existing live audit envelope is invalid"
                    ) from exc
                if not isinstance(existing_envelope, dict):
                    raise RuntimeError("existing live audit envelope is invalid")
                immutable_existing = {
                    key: value
                    for key, value in existing_envelope.items()
                    if key != "created_at"
                }
                immutable_new = {
                    key: value for key, value in envelope.items() if key != "created_at"
                }
                if immutable_existing != immutable_new:
                    raise RuntimeError("existing live audit envelope conflicts")
                reuse_envelope = True
            durable_evidence = self._normalized_durable_evidence(
                identity=identity,
                envelope_path=envelope_path,
                report=report,
                persistence=persistence,
                evidence=evidence,
                verification_identity=verification_identity,
            )
            try:
                old_evidence, _old_evidence_stat = (
                    self._read_owned_regular_bytes(self._evidence_path)
                )
            except FileNotFoundError:
                old_evidence = None
            reuse_evidence = False
            if old_evidence is not None:
                try:
                    existing_evidence = json.loads(old_evidence)
                except (UnicodeDecodeError, json.JSONDecodeError):
                    # Invalid bytes at the mutable evidence path may be
                    # repaired by this ledger-proven publication.
                    pass
                else:
                    if existing_evidence == durable_evidence:
                        reuse_evidence = True
                    else:
                        try:
                            _payload, _path, existing_identity = (
                                self._validate_evidence(existing_evidence)
                            )
                        except (OSError, RuntimeError, TypeError, ValueError):
                            # Canonical JSON can still be structurally or
                            # cryptographically invalid.  It has the same
                            # repair policy as malformed bytes.
                            pass
                        else:
                            if identity.sequence <= existing_identity.sequence:
                                raise RuntimeError(
                                    "existing live audit evidence conflicts"
                                )
            if not reuse_envelope:
                published_envelope_identity = self._write_mutable_json(
                    envelope_path,
                    envelope,
                    sort_keys=True,
                )
            else:
                published_envelope_identity = None
                self._fsync_directory(envelope_path.parent)
            published_evidence_identity: _FileIdentity | None = None
            try:
                if reuse_evidence:
                    self._fsync_directory(self._evidence_path.parent)
                else:
                    published_evidence_identity = self._write_mutable_json(
                        self._evidence_path,
                        durable_evidence,
                        sort_keys=True,
                    )
                self._validate_root_identity()
                self._validate_evidence_parent_identity()
                self._require_publication_order_guard()
            except BaseException:
                if published_evidence_identity is not None:
                    try:
                        self._rollback_mutable_replace(
                            self._evidence_path,
                            old_evidence,
                            published_evidence_identity,
                            allow_detached_authority=True,
                        )
                    except BaseException:
                        pass
                if reuse_envelope:
                    pass
                elif old_envelope is None:
                    assert published_envelope_identity is not None
                    self._remove_identity_safe(
                        envelope_path,
                        published_envelope_identity,
                        allow_detached_authority=True,
                    )
                    self._fsync_pinned_directory(envelope_path.parent)
                else:
                    assert published_envelope_identity is not None
                    self._rollback_mutable_replace(
                        envelope_path,
                        old_envelope,
                        published_envelope_identity,
                        allow_detached_authority=True,
                    )
                raise
            self._require_publication_order_guard()
            self._latest_evidence = copy.deepcopy(durable_evidence)
            self._current_envelope = envelope_path
            self._current_identity = identity
            self._evidence_state = "valid"
            self._compatibility_evidence_override = False
            self._invalidated_legacy_identity = None
            self._legacy_proof_token = None
        try:
            self.prune_best_effort()
        except Exception:
            # Retention is maintenance after the evidence reference is durable.
            pass
        return AuditPublication(
            identity=identity,
            envelope_path=envelope_path,
            evidence=copy.deepcopy(durable_evidence),
            published=True,
        )

    def _build_live_envelope(
        self,
        *,
        identity: AuditPublicationIdentity,
        report: Mapping[str, Any],
        persistence: Mapping[str, Any],
        created_at: str,
    ) -> dict[str, Any]:
        normalized_report = self._normalize_report_identity(report)
        if normalized_report["block_height"] != identity.block_height:
            raise RuntimeError("audit report block height does not match publication")
        report_digest = _canonical_hex(
            normalized_report.get("audit_bundle_sha256_hex"),
            name="audit report digest",
        )
        persistence_digest = _canonical_hex(
            persistence.get("audit_bundle_sha256"),
            name="persistence digest",
        )
        if not hmac.compare_digest(report_digest, persistence_digest):
            raise RuntimeError("audit report and persistence digests differ")
        body_uri = str(persistence.get("body_uri") or "")
        if body_uri:
            body_path = self.resolve_owned_path(body_uri)
            expected_body_path = self.body_path(identity.block_hash, persistence_digest)
            if body_path != expected_body_path:
                raise RuntimeError(
                    "persistence body URI does not match publication identity"
                )
            body_uri = str(body_path)
        return {
            "schema": LIVE_ENVELOPE_SCHEMA,
            "block_hash": identity.block_hash,
            "block_height": identity.block_height,
            "audit_bundle_sha256": persistence_digest,
            "body_uri": body_uri,
            "body_filename": Path(body_uri).name if body_uri else None,
            "coinbase_txid": normalized_report.get("coinbase_txid"),
            "coinbase_manifest_sha256": normalized_report.get(
                "coinbase_manifest_sha256_hex"
            ),
            "coinbase_tx_hex": normalized_report.get("coinbase_tx_hex"),
            "coinbase_value_sats": normalized_report.get("coinbase_value_sats"),
            "created_at": created_at,
        }

    def _normalized_durable_evidence(
        self,
        *,
        identity: AuditPublicationIdentity,
        envelope_path: Path,
        report: Mapping[str, Any],
        persistence: Mapping[str, Any],
        evidence: Mapping[str, Any],
        verification_identity: Mapping[str, Any],
    ) -> dict[str, Any]:
        normalized_report = self._normalize_report_identity(report)
        normalized_verification = self._normalize_verification_identity(
            dict(verification_identity),
            report=normalized_report,
        )
        normalized_persistence = copy.deepcopy(dict(persistence))
        normalized_persistence["audit_bundle_sha256"] = _canonical_hex(
            normalized_persistence.get("audit_bundle_sha256"),
            name="persistence digest",
        )
        body_uri = str(normalized_persistence.get("body_uri") or "")
        if body_uri:
            normalized_persistence["body_uri"] = str(self.resolve_owned_path(body_uri))
        durable = copy.deepcopy(dict(evidence))
        durable.update(
            {
                "schema": LIVE_EVIDENCE_SCHEMA,
                "block_hash": identity.block_hash,
                "block_height": identity.block_height,
                "audit_bundle_path": str(envelope_path),
                "audit_publication_identity": identity.to_json(),
                "audit_report": normalized_report,
                "audit_verification_identity": normalized_verification,
                "persistence": normalized_persistence,
                "coinbase_txid": normalized_report["coinbase_txid"],
                "coinbase_manifest_sha256_hex": normalized_report[
                    "coinbase_manifest_sha256_hex"
                ],
                "coinbase_tx_hex": normalized_report["coinbase_tx_hex"],
                "coinbase_value_sats": normalized_report["coinbase_value_sats"],
            }
        )
        return durable

    def latest_evidence(self) -> dict[str, Any] | None:
        with self._lock:
            if not self._directory_authority_is_current():
                return None
            if self._compatibility_evidence_override:
                return copy.deepcopy(self._latest_evidence)
            if self._evidence_state == "legacy_unproven":
                return None
            if self._latest_evidence is not None:
                return copy.deepcopy(self._latest_evidence)
            if self._evidence_state == "invalid":
                return None
        self._load_current_evidence()
        with self._lock:
            if not self._directory_authority_is_current():
                return None
            return copy.deepcopy(self._latest_evidence)

    def set_latest_evidence_for_compatibility(
        self,
        payload: Mapping[str, Any] | None,
    ) -> None:
        with self._lock:
            self._compatibility_evidence_override = True
            self._invalidated_legacy_identity = None
            self._legacy_proof_token = None
            if payload is None:
                self._latest_evidence = None
                self._current_envelope = None
                self._current_identity = None
                self._evidence_state = "absent"
                return
            copied = copy.deepcopy(dict(payload))
            try:
                latest, envelope, identity = self._validate_evidence(copied)
            except (OSError, RuntimeError, TypeError, ValueError):
                self._latest_evidence = copied
                self._current_envelope = None
                self._current_identity = None
                self._evidence_state = "invalid"
                return
            self._latest_evidence = latest
            self._current_envelope = envelope
            self._current_identity = identity
            self._evidence_state = self._loaded_evidence_state(latest, identity)

    def _load_current_evidence(self) -> None:
        with self._lock:
            self._load_current_evidence_locked()

    def _load_current_evidence_locked(self) -> None:
        try:
            self._validate_evidence_parent_identity()
            evidence_bytes, _value = self._read_owned_regular_bytes(self._evidence_path)
        except FileNotFoundError:
            self._latest_evidence = None
            self._current_envelope = None
            self._current_identity = None
            self._evidence_state = "absent"
            return
        except (OSError, RuntimeError):
            self._latest_evidence = None
            self._current_envelope = None
            self._current_identity = None
            self._evidence_state = "invalid"
            return
        try:
            payload = json.loads(evidence_bytes.decode("utf-8"))
            parsed = self._validate_evidence(payload)
        except (OSError, UnicodeDecodeError, json.JSONDecodeError, RuntimeError, TypeError, ValueError):
            self._latest_evidence = None
            self._current_envelope = None
            self._current_identity = None
            self._evidence_state = "invalid"
            return
        self._latest_evidence, self._current_envelope, self._current_identity = parsed
        self._evidence_state = self._loaded_evidence_state(
            self._latest_evidence,
            self._current_identity,
        )

    def _reload_current_evidence_locked(self) -> None:
        """Reload peer state while retaining an exact in-process legacy proof."""

        prior_token = self._legacy_proof_token
        self._load_current_evidence_locked()
        if (
            prior_token is not None
            and self._evidence_state == "legacy_unproven"
            and self._current_identity == prior_token.identity
        ):
            try:
                current_token = self._capture_legacy_proof_token(
                    prior_token.identity
                )
            except (OSError, RuntimeError):
                current_token = None
            if current_token == prior_token:
                self._evidence_state = "legacy_proven"
                return
        self._legacy_proof_token = None

    def _capture_legacy_proof_token(
        self,
        identity: AuditPublicationIdentity,
    ) -> _LegacyProofToken:
        if self._current_envelope is None:
            raise RuntimeError("legacy audit envelope is unavailable")
        evidence_bytes, evidence_stat = self._read_owned_regular_bytes(
            self._evidence_path
        )
        envelope_bytes, envelope_stat = self._read_owned_regular_bytes(
            self._current_envelope
        )
        return _LegacyProofToken(
            identity=identity,
            evidence_file=_FileIdentity.from_stat(evidence_stat),
            evidence_sha256=_sha256_bytes(evidence_bytes),
            envelope_file=_FileIdentity.from_stat(envelope_stat),
            envelope_sha256=_sha256_bytes(envelope_bytes),
        )

    def _validate_evidence(
        self,
        payload: object,
    ) -> tuple[dict[str, Any], Path, AuditPublicationIdentity]:
        if not isinstance(payload, dict) or payload.get("schema") != LIVE_EVIDENCE_SCHEMA:
            raise RuntimeError("invalid evidence schema")
        self._validate_root_identity()
        raw_block_hash = payload.get("block_hash")
        block_hash = _canonical_hex(raw_block_hash, name="block_hash")
        if raw_block_hash != block_hash:
            raise RuntimeError("evidence block hash is not canonical")
        raw_block_height = payload.get("block_height")
        if (
            isinstance(raw_block_height, bool)
            or not isinstance(raw_block_height, int)
            or raw_block_height < 0
        ):
            raise RuntimeError("evidence block height is invalid")
        block_height = raw_block_height
        raw_envelope_path = payload.get("audit_bundle_path")
        if not isinstance(raw_envelope_path, str):
            raise RuntimeError("evidence envelope path is not canonical")
        envelope_path = Path(raw_envelope_path).absolute()
        expected_path = self.live_envelope_path(
            block_height=block_height,
            block_hash=block_hash,
        )
        if envelope_path != expected_path or raw_envelope_path != str(expected_path):
            raise RuntimeError("evidence envelope path is not canonical")
        envelope_bytes, _envelope_stat = self._read_owned_regular_bytes(envelope_path)
        envelope = json.loads(envelope_bytes.decode("utf-8"))
        if (
            not isinstance(envelope, dict)
            or envelope.get("schema") != LIVE_ENVELOPE_SCHEMA
            or envelope.get("block_hash") != block_hash
            or isinstance(envelope.get("block_height"), bool)
            or not isinstance(envelope.get("block_height"), int)
            or envelope.get("block_height") != block_height
        ):
            raise RuntimeError("evidence envelope identity mismatch")
        report = payload.get("audit_report")
        persistence = payload.get("persistence")
        if not isinstance(report, dict) or not isinstance(persistence, dict):
            raise RuntimeError("evidence report and persistence are required")
        normalized_report = self._normalize_report_identity(report)
        if report != normalized_report:
            raise RuntimeError("evidence audit report is not canonical")
        if normalized_report["block_height"] != block_height:
            raise RuntimeError("evidence report block height mismatch")
        report_digest = _canonical_hex(
            normalized_report.get("audit_bundle_sha256_hex"),
            name="audit report digest",
        )
        raw_persistence_digest = persistence.get("audit_bundle_sha256")
        persistence_digest = _canonical_hex(
            raw_persistence_digest,
            name="persistence digest",
        )
        raw_envelope_digest = envelope.get("audit_bundle_sha256")
        envelope_digest = _canonical_hex(
            raw_envelope_digest,
            name="envelope digest",
        )
        if (
            raw_persistence_digest != persistence_digest
            or raw_envelope_digest != envelope_digest
        ):
            raise RuntimeError("evidence digest is not canonical")
        if not (
            hmac.compare_digest(report_digest, persistence_digest)
            and hmac.compare_digest(report_digest, envelope_digest)
        ):
            raise RuntimeError("evidence digest mismatch")
        raw_persistence_body_uri = persistence.get("body_uri")
        raw_envelope_body_uri = envelope.get("body_uri")
        if not isinstance(raw_persistence_body_uri, str) or not isinstance(
            raw_envelope_body_uri,
            str,
        ):
            raise RuntimeError("evidence body URI is not canonical")
        persistence_body_uri = raw_persistence_body_uri
        envelope_body_uri = raw_envelope_body_uri
        if persistence_body_uri:
            body_path = self.resolve_owned_path(persistence_body_uri)
            expected_body_path = self.body_path(block_hash, persistence_digest)
            if body_path != expected_body_path:
                raise RuntimeError("evidence body URI identity mismatch")
            canonical_body_uri = str(body_path)
            if persistence_body_uri != canonical_body_uri:
                raise RuntimeError("evidence body URI is not canonical")
            persistence_body_uri = canonical_body_uri
        if envelope_body_uri:
            canonical_envelope_body_uri = str(
                self.resolve_owned_path(envelope_body_uri)
            )
            if envelope_body_uri != canonical_envelope_body_uri:
                raise RuntimeError("envelope body URI is not canonical")
            envelope_body_uri = canonical_envelope_body_uri
        if (
            envelope_body_uri != persistence_body_uri
            or envelope.get("body_filename")
            != (Path(persistence_body_uri).name if persistence_body_uri else None)
        ):
            raise RuntimeError("evidence and envelope body URIs differ")
        coinbase_bindings = (
            ("coinbase_txid", "coinbase_txid"),
            ("coinbase_manifest_sha256_hex", "coinbase_manifest_sha256"),
            ("coinbase_tx_hex", "coinbase_tx_hex"),
            ("coinbase_value_sats", "coinbase_value_sats"),
        )
        for report_key, envelope_key in coinbase_bindings:
            expected_value = normalized_report[report_key]
            evidence_value = payload.get(report_key)
            envelope_value = envelope.get(envelope_key)
            if report_key == "coinbase_value_sats":
                if (
                    isinstance(evidence_value, bool)
                    or not isinstance(evidence_value, int)
                    or isinstance(envelope_value, bool)
                    or not isinstance(envelope_value, int)
                ):
                    raise RuntimeError("evidence coinbase value is invalid")
            elif report_key == "coinbase_tx_hex":
                canonical_evidence_value = _canonical_hex_bytes(
                    evidence_value,
                    name="evidence coinbase_tx_hex",
                )
                canonical_envelope_value = _canonical_hex_bytes(
                    envelope_value,
                    name="envelope coinbase_tx_hex",
                )
                if (
                    evidence_value != canonical_evidence_value
                    or envelope_value != canonical_envelope_value
                ):
                    raise RuntimeError("evidence coinbase identity is not canonical")
                evidence_value = canonical_evidence_value
                envelope_value = canonical_envelope_value
            else:
                canonical_evidence_value = _canonical_hex(
                    evidence_value,
                    name=report_key,
                )
                canonical_envelope_value = _canonical_hex(
                    envelope_value,
                    name=envelope_key,
                )
                if (
                    evidence_value != canonical_evidence_value
                    or envelope_value != canonical_envelope_value
                ):
                    raise RuntimeError("evidence coinbase identity is not canonical")
                evidence_value = canonical_evidence_value
                envelope_value = canonical_envelope_value
            if evidence_value != expected_value or envelope_value != expected_value:
                raise RuntimeError("evidence coinbase identity mismatch")
        identity_payload = payload.get("audit_publication_identity")
        if not isinstance(identity_payload, dict):
            # Older durable evidence remains readable but starts at sequence 0.
            identity = AuditPublicationIdentity(0, block_height, block_hash)
        else:
            raw_sequence = identity_payload.get("sequence")
            raw_identity_height = identity_payload.get("block_height")
            if (
                isinstance(raw_sequence, bool)
                or not isinstance(raw_sequence, int)
                or isinstance(raw_identity_height, bool)
                or not isinstance(raw_identity_height, int)
            ):
                raise RuntimeError("invalid evidence publication identity")
            identity = AuditPublicationIdentity(
                raw_sequence,
                raw_identity_height,
                _canonical_hex(
                    identity_payload.get("block_hash"),
                    name="publication block hash",
                ),
            )
            if (
                identity.sequence < 0
                or identity.block_height != block_height
                or identity.block_hash != block_hash
                or identity_payload != identity.to_json()
            ):
                raise RuntimeError("invalid evidence publication identity")
        normalized_payload = copy.deepcopy(payload)
        verification_payload = payload.get("audit_verification_identity")
        if identity.sequence > 0:
            if (
                isinstance(verification_payload, dict)
                and verification_payload.get("schema")
                == LEGACY_VERIFICATION_UNAVAILABLE_SCHEMA
            ):
                normalized_verification = (
                    self._normalize_legacy_verification_marker(
                        verification_payload,
                        identity=identity,
                        report=normalized_report,
                    )
                )
            else:
                normalized_verification = self._normalize_verification_identity(
                    verification_payload,
                    report=normalized_report,
                )
            normalized_payload["audit_verification_identity"] = (
                normalized_verification
            )
        elif verification_payload is not None:
            # Sequence-zero evidence is the only supported legacy form.  If a
            # producer supplied a verification identity anyway, validate it
            # rather than allowing an unchecked field to become durable state.
            normalized_payload["audit_verification_identity"] = (
                self._normalize_verification_identity(
                    verification_payload,
                    report=normalized_report,
                )
            )
        self._validate_root_identity()
        self._validate_evidence_parent_identity()
        return normalized_payload, envelope_path, identity

    def adopt_legacy_publication_identity(
        self,
        identity: AuditPublicationIdentity,
        *,
        publication_floor_sequence: int,
    ) -> None:
        """Record a ledger proof that must be revalidated after every restart."""

        self._require_publication_order_guard()
        if (
            isinstance(publication_floor_sequence, bool)
            or not isinstance(publication_floor_sequence, int)
            or publication_floor_sequence < 0
        ):
            raise ValueError("publication floor sequence must be a non-negative integer")
        with self._lock:
            self._validate_root_identity()
            self._validate_evidence_parent_identity()
            if not self._compatibility_evidence_override:
                self._load_current_evidence_locked()
            if self._evidence_state not in {
                "legacy",
                "legacy_unproven",
            } or self._latest_evidence is None:
                return
            if identity.sequence > publication_floor_sequence:
                raise RuntimeError(
                    "legacy audit publication sequence exceeds the durable ledger floor"
                )
            if identity.sequence < publication_floor_sequence:
                self.invalidate_unprovable_legacy_evidence()
                return
            if (
                identity.sequence <= 0
                or identity.block_hash != self._latest_evidence.get("block_hash")
                or identity.block_height
                != int(self._latest_evidence.get("block_height", -1))
            ):
                raise RuntimeError("legacy evidence ledger identity mismatch")
            upgraded = copy.deepcopy(self._latest_evidence)
            upgraded["audit_publication_identity"] = identity.to_json()
            report = upgraded.get("audit_report")
            if not isinstance(report, dict):
                raise RuntimeError("legacy audit report is unavailable")
            upgraded["audit_verification_identity"] = (
                self._legacy_verification_marker(
                    identity=identity,
                    report=report,
                )
            )
            old_evidence, _old_stat = self._read_owned_regular_bytes(
                self._evidence_path
            )
            published_identity = self._write_mutable_json(
                self._evidence_path,
                upgraded,
                sort_keys=True,
            )
            try:
                self._validate_root_identity()
                self._validate_evidence_parent_identity()
            except BaseException:
                self._rollback_mutable_replace(
                    self._evidence_path,
                    old_evidence,
                    published_identity,
                    allow_detached_authority=True,
                )
                raise
            self._latest_evidence = upgraded
            self._current_identity = identity
            self._evidence_state = "legacy_proven"
            self._invalidated_legacy_identity = None
            self._legacy_proof_token = self._capture_legacy_proof_token(identity)

    def invalidate_unprovable_legacy_evidence(self) -> None:
        """Drop ordering/pin authority while allowing a new durable repair."""

        with self._lock:
            if self._evidence_state not in {"legacy", "legacy_unproven"}:
                return
            self._invalidated_legacy_identity = self._current_identity
            self._legacy_proof_token = None
            self._latest_evidence = None
            self._current_envelope = None
            self._current_identity = None
            self._evidence_state = "invalid"

    def prune_best_effort(
        self,
        *,
        keep_live_path: Path | None = None,
    ) -> RetentionResult:
        try:
            with self.publication_order_guard():
                with self._lock:
                    if not self._compatibility_evidence_override:
                        # A different coordinator process may have published a
                        # newer reference. Reconcile it under the same flock
                        # immediately before deriving live-deletion pins.
                        self._reload_current_evidence_locked()
                return self._prune_best_effort_guarded(
                    keep_live_path=keep_live_path,
                )
        except (OSError, RuntimeError):
            return RetentionResult(errors=1)

    def _prune_best_effort_guarded(
        self,
        *,
        keep_live_path: Path | None = None,
    ) -> RetentionResult:
        self._require_publication_order_guard()
        live_removed = 0
        candidate_removed = 0
        errors = 0
        with self._lock:
            live_authorized = self._directory_authority_is_current()
            pins = (
                {
                    path
                    for path in (self._current_envelope,)
                    if path is not None
                }
                if live_authorized
                and self._evidence_state in {"valid", "legacy_proven"}
                else set()
            )
            if keep_live_path is not None:
                raw_keep = Path(keep_live_path).absolute()
                candidate_keep = raw_keep.parent.resolve() / raw_keep.name
                if (
                    candidate_keep.parent == self._root
                    and _LIVE_RE.fullmatch(candidate_keep.name)
                ):
                    pins.add(candidate_keep)
            active_paths = {entry[0] for entry in self._active_candidates.values()}
            live_retention = self._live_bundle_retention
            candidate_retention = self._candidate_retention_seconds
            evidence_state = self._evidence_state if live_authorized else "invalid"
        try:
            self._validate_root_identity()
            entries = sorted(
                (self._root / name for name in os.listdir(self._root_fd)),
                key=lambda value: value.name,
            )
        except (OSError, RuntimeError):
            return RetentionResult(errors=1)
        live: list[tuple[int, int, str, Path, _FileIdentity]] = []
        now = self._wall_time()
        for path in entries:
            try:
                value = self._owned_lstat(path)
            except RuntimeError:
                errors += 1
                return RetentionResult(0, candidate_removed, errors)
            except (FileNotFoundError, OSError):
                errors += 1
                continue
            if not stat.S_ISREG(value.st_mode):
                continue
            live_match = _LIVE_RE.fullmatch(path.name)
            if live_match:
                live.append(
                    (
                        value.st_mtime_ns,
                        int(live_match.group("height")),
                        live_match.group("block"),
                        path,
                        _FileIdentity.from_stat(value),
                    )
                )
                continue
            if not (
                _CANDIDATE_RE.fullmatch(path.name)
                or _LEGACY_CANDIDATE_RE.fullmatch(path.name)
                or _LEGACY_HIDDEN_CANDIDATE_RE.fullmatch(path.name)
            ):
                continue
            if path in active_paths:
                continue
            if candidate_retention != 0 and now - value.st_mtime <= candidate_retention:
                continue
            try:
                with self._lock:
                    if not self._directory_authority_is_current():
                        errors += 1
                        break
                    current_active_paths = {
                        entry[0] for entry in self._active_candidates.values()
                    }
                    if path in current_active_paths:
                        continue
                    if self._unlink_scanned_owned(
                        path,
                        _FileIdentity.from_stat(value),
                    ):
                        candidate_removed += 1
            except (OSError, RuntimeError):
                errors += 1
        if evidence_state not in {"valid", "legacy_proven"}:
            return RetentionResult(0, candidate_removed, errors)
        live.sort(key=lambda item: (item[0], item[1], item[2], item[3].name), reverse=True)
        retained = 0
        for _mtime, _height, _block, path, identity in live:
            if path in pins:
                continue
            if live_retention < 0 or retained < max(live_retention - len(pins), 0):
                retained += 1
                continue
            try:
                with self._lock:
                    self._require_publication_order_guard()
                    if not self._directory_authority_is_current():
                        errors += 1
                        break
                    if path == self._current_envelope:
                        continue
                    if self._unlink_scanned_owned(
                        path,
                        identity,
                        require_all_authorities=True,
                    ):
                        live_removed += 1
            except (OSError, RuntimeError):
                errors += 1
        try:
            self._validate_root_identity()
            self._validate_evidence_parent_identity()
        except RuntimeError:
            errors += 1
        return RetentionResult(live_removed, candidate_removed, errors)

    def _unlink_scanned_owned(
        self,
        path: Path,
        identity: _FileIdentity,
        *,
        require_all_authorities: bool = False,
    ) -> bool:
        if require_all_authorities and not self._directory_authority_is_current():
            raise RuntimeError("audit directory authority changed before live prune")
        return self._remove_identity_safe(path, identity)

    def _remove_identity_safe(
        self,
        path: Path,
        identity: _FileIdentity,
        *,
        allow_detached_authority: bool = False,
    ) -> bool:
        if not allow_detached_authority:
            self._validate_owned_parent(path)
        try:
            current = self._owned_lstat(path)
        except FileNotFoundError:
            return False
        if not stat.S_ISREG(current.st_mode):
            # Hard-link restoration is unavailable for directories and is not
            # portable for symlinks.  Never relocate an unowned nonregular
            # replacement merely to discover that cleanup lacks authority.
            return False
        if not identity.matches(current):
            # A replacement that was already present at the precheck is not
            # ours to relocate, even temporarily.
            return False
        quarantine = path.with_name(f".{path.name}.{uuid.uuid4().hex}.cleanup")
        try:
            self._owned_replace(path, quarantine)
        except FileNotFoundError:
            return False
        try:
            moved = self._owned_lstat(quarantine)
            if identity.matches(moved):
                self._owned_unlink(quarantine)
                return True
            # A replacement won the race. Restore it when the original name is
            # free; otherwise preserve it under the quarantine name.
            try:
                self._owned_link(quarantine, path)
            except OSError:
                return False
            self._owned_unlink(quarantine)
            return False
        except BaseException:
            # Never turn an uncertain identity into deletion authority.
            raise

    def _write_mutable_json(
        self,
        path: Path,
        payload: Mapping[str, Any],
        *,
        sort_keys: bool,
    ) -> _FileIdentity:
        body = _json_bytes(payload, sort_keys=sort_keys)
        return self._write_mutable_bytes(path, body)

    def _write_mutable_bytes(self, path: Path, payload: bytes) -> _FileIdentity:
        path = Path(path).absolute()
        resolved_parent = path.parent.resolve(strict=True)
        if not (
            resolved_parent == self._root
            or path == self._evidence_path
        ):
            raise RuntimeError("mutable audit target is outside the owned store")
        with self._lock:
            if path == self._evidence_path:
                self._validate_evidence_parent_identity()
            else:
                self._validate_root_identity()
            old_bytes: bytes | None
            try:
                old_bytes, _old_stat = self._read_owned_regular_bytes(path)
            except FileNotFoundError:
                old_bytes = None
            tmp_path = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
            temp_identity: _FileIdentity | None = None
            published = False
            try:
                fd = self._owned_open(
                    tmp_path,
                    os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
                    0o600,
                )
                temp_identity = _FileIdentity.from_stat(os.fstat(fd))
                with os.fdopen(fd, "wb") as handle:
                    try:
                        handle.write(payload)
                        handle.flush()
                        os.fsync(handle.fileno())
                    finally:
                        temp_identity = _FileIdentity.from_stat(
                            os.fstat(handle.fileno())
                        )
                self._owned_replace(tmp_path, path)
                published = True
                try:
                    self._fsync_directory(path.parent)
                    if path == self._evidence_path:
                        self._validate_evidence_parent_identity()
                    else:
                        self._validate_root_identity()
                except BaseException:
                    self._rollback_mutable_replace(
                        path,
                        old_bytes,
                        temp_identity,
                        allow_detached_authority=True,
                    )
                    raise
                return temp_identity
            finally:
                if not published and temp_identity is not None:
                    self._remove_identity_safe(
                        tmp_path,
                        temp_identity,
                        allow_detached_authority=True,
                    )

    def _rollback_mutable_replace(
        self,
        path: Path,
        old_bytes: bytes | None,
        published_identity: _FileIdentity,
        *,
        allow_detached_authority: bool = False,
    ) -> None:
        removed = self._remove_identity_safe(
            path,
            published_identity,
            allow_detached_authority=allow_detached_authority,
        )
        if not removed:
            # A replacement won the race. Preserve it and do not restore stale
            # bytes over an inode this operation never owned.
            return
        if old_bytes is None:
            if allow_detached_authority:
                self._fsync_pinned_directory(path.parent)
            else:
                self._fsync_directory(path.parent)
            return
        rollback = path.with_name(f".{path.name}.{uuid.uuid4().hex}.rollback")
        rollback_identity: _FileIdentity | None = None
        try:
            fd = self._owned_open(
                rollback,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
                0o600,
            )
            rollback_identity = _FileIdentity.from_stat(os.fstat(fd))
            with os.fdopen(fd, "wb") as handle:
                try:
                    handle.write(old_bytes)
                    handle.flush()
                    os.fsync(handle.fileno())
                finally:
                    rollback_identity = _FileIdentity.from_stat(
                        os.fstat(handle.fileno())
                    )
            try:
                self._owned_link(rollback, path)
            except FileExistsError:
                # Preserve a concurrent replacement rather than overwriting it.
                return
            if allow_detached_authority:
                self._fsync_pinned_directory(path.parent)
            else:
                self._fsync_directory(path.parent)
        finally:
            if rollback_identity is not None:
                self._remove_identity_safe(
                    rollback,
                    rollback_identity,
                    allow_detached_authority=allow_detached_authority,
                )

    def _write_immutable_bytes(self, path: Path, payload: bytes) -> None:
        path = Path(path).absolute()
        if (
            path.parent.resolve(strict=True) != self._root
            or self.artifact_kind(path.name) not in {"body", "share_segment"}
        ):
            raise RuntimeError("immutable audit target is not an owned body or segment")
        with self._lock:
            self._validate_root_identity()
            try:
                existing = self._owned_lstat(path)
            except FileNotFoundError:
                existing = None
            if existing is not None:
                if not stat.S_ISREG(existing.st_mode) or not self.file_matches_bytes(
                    path,
                    payload,
                ):
                    raise RuntimeError(
                        f"existing audit artifact does not match payload at {path}"
                    )
                # The prior writer may have failed between link/rename and its
                # directory fsync.  An idempotent retry is the durability
                # repair boundary, even though no bytes need to change.
                self._fsync_directory(path.parent)
                return
            tmp_path = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
            temp_identity: _FileIdentity | None = None
            linked_by_this_call = False
            try:
                fd = self._owned_open(
                    tmp_path,
                    os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
                    0o600,
                )
                temp_identity = _FileIdentity.from_stat(os.fstat(fd))
                with os.fdopen(fd, "wb") as handle:
                    try:
                        handle.write(payload)
                        handle.flush()
                        os.fsync(handle.fileno())
                    finally:
                        temp_identity = _FileIdentity.from_stat(
                            os.fstat(handle.fileno())
                        )
                try:
                    self._owned_link(tmp_path, path)
                    linked_by_this_call = True
                except FileExistsError:
                    if not self.file_matches_bytes(path, payload):
                        raise RuntimeError(
                            f"existing audit artifact does not match payload at {path}"
                        )
                try:
                    self._fsync_directory(path.parent)
                except BaseException:
                    if linked_by_this_call:
                        # Revoke only the inode linked by this call.  A racing
                        # replacement is never deleted or overwritten.
                        self._remove_identity_safe(
                            path,
                            temp_identity,
                            allow_detached_authority=True,
                        )
                        try:
                            self._fsync_directory(path.parent)
                        except BaseException:
                            # Preserve the original durability failure; the
                            # retry path always fsyncs an equal existing file.
                            pass
                    raise
                self._validate_root_identity()
            finally:
                if temp_identity is not None:
                    self._remove_identity_safe(
                        tmp_path,
                        temp_identity,
                        allow_detached_authority=True,
                    )

    def _fsync_directory(self, path: Path) -> None:
        path = Path(path).absolute()
        if path == self._root:
            self._validate_root_identity()
            os.fsync(self._root_fd)
            self._validate_root_identity()
            return
        if path == self._evidence_path.parent:
            self._validate_evidence_parent_identity()
            os.fsync(self._evidence_parent_fd)
            self._validate_evidence_parent_identity()
            return
        raise RuntimeError("audit fsync target has no pinned authority")

    def _fsync_pinned_directory(self, path: Path) -> None:
        path = Path(path).absolute()
        if path == self._root:
            os.fsync(self._root_fd)
            return
        if path == self._evidence_path.parent:
            os.fsync(self._evidence_parent_fd)
            return
        raise RuntimeError("audit fsync target has no pinned authority")

    def file_matches_bytes(self, path: Path, expected: bytes) -> bool:
        try:
            payload, value = self._read_owned_regular_bytes(path)
            if value.st_size != len(expected):
                return False
            return hmac.compare_digest(payload, expected)
        except OSError:
            return False

    @staticmethod
    def file_sha256_hex(path: Path) -> str:
        payload, _value = AuditArtifactStore.read_regular_bytes(path)
        return _sha256_bytes(payload)

    @staticmethod
    def read_regular_bytes(path: Path) -> tuple[bytes, os.stat_result]:
        fd = os.open(
            path,
            os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
        )
        return AuditArtifactStore._read_regular_bytes_fd(fd)

    @staticmethod
    def _read_regular_bytes_fd(fd: int) -> tuple[bytes, os.stat_result]:
        try:
            before = os.fstat(fd)
            if not stat.S_ISREG(before.st_mode):
                raise OSError("audit artifact is not a regular file")
            chunks: list[bytes] = []
            while True:
                chunk = os.read(fd, 1024 * 1024)
                if not chunk:
                    break
                chunks.append(chunk)
            after = os.fstat(fd)
            if (
                before.st_dev != after.st_dev
                or before.st_ino != after.st_ino
                or before.st_size != after.st_size
                or before.st_mtime_ns != after.st_mtime_ns
                or before.st_ctime_ns != after.st_ctime_ns
            ):
                raise OSError("audit artifact changed while reading")
            return b"".join(chunks), after
        finally:
            os.close(fd)

    # External body and share-segment capability used by the ledger.  Database
    # lease checks remain in the ledger; paths, encodings, and bytes stay here.

    def canonical_audit_bundle_bytes(self, final_bundle: dict[str, Any]) -> bytes:
        return canonical_audit_bundle_bytes(final_bundle, self._canonicalizer)

    def canonical_audit_body_bytes_for_sha(
        self,
        final_bundle: dict[str, Any],
        audit_bundle_sha256: str,
    ) -> bytes:
        expected = _canonical_hex(
            audit_bundle_sha256,
            name="audit_bundle_sha256",
        )
        body = self.canonical_audit_bundle_bytes(final_bundle)
        actual = _sha256_bytes(body)
        if not hmac.compare_digest(actual, expected):
            raise RuntimeError(
                f"audit bundle sha256 mismatch: expected {expected}, got {actual}"
            )
        return body

    @staticmethod
    def storage_json_bytes(payload: Mapping[str, Any]) -> bytes:
        return _json_bytes(payload)

    def audit_share_segment_payload(
        self,
        *,
        first_share_seq: int,
        last_share_seq: int,
        shares: list[Any],
    ) -> dict[str, Any]:
        return {
            "schema": AUDIT_SHARE_SEGMENT_SCHEMA,
            "first_share_seq": first_share_seq,
            "last_share_seq": last_share_seq,
            "share_count": len(shares),
            "shares": shares,
        }

    def write_audit_share_segment(
        self,
        *,
        first_share_seq: int,
        last_share_seq: int,
        shares: list[Any],
    ) -> tuple[str, str]:
        self._validate_share_range(first_share_seq, last_share_seq, shares)
        segment = self.audit_share_segment_payload(
            first_share_seq=first_share_seq,
            last_share_seq=last_share_seq,
            shares=shares,
        )
        payload = self.storage_json_bytes(segment)
        digest = _sha256_bytes(payload)
        path = self._root / (
            f"prism-audit-share-segment-{first_share_seq}-{last_share_seq}-{digest}.json"
        )
        if not _SHARE_CONTENT_RE.fullmatch(path.name):
            raise RuntimeError("invalid audit share segment bounds")
        self._write_immutable_bytes(path, payload)
        self._validate_root_identity()
        return str(path), digest

    def write_audit_share_segment_range(
        self,
        *,
        segment_first_share_seq: int,
        segment_last_share_seq: int,
        first_share_seq: int,
        last_share_seq: int,
        shares: list[Any],
    ) -> tuple[str, str]:
        if not shares:
            raise RuntimeError("audit share segment range cannot be empty")
        if (
            segment_first_share_seq <= 0
            or segment_last_share_seq < segment_first_share_seq
            or first_share_seq < segment_first_share_seq
            or last_share_seq > segment_last_share_seq
        ):
            raise RuntimeError("audit share range is outside its segment slot")
        self._validate_share_range(first_share_seq, last_share_seq, shares)
        path = self._root / (
            "prism-audit-share-segment-slot-"
            f"{segment_first_share_seq}-{segment_last_share_seq}.json"
        )
        if not _SHARE_SLOT_RE.fullmatch(path.name):
            raise RuntimeError("invalid audit share segment slot bounds")
        incoming = self.audit_share_segment_payload(
            first_share_seq=first_share_seq,
            last_share_seq=last_share_seq,
            shares=shares,
        )
        incoming_bytes = self.storage_json_bytes(incoming)
        range_digest = _sha256_bytes(incoming_bytes)
        with self._lock:
            self._validate_root_identity()
            if self.file_matches_bytes(path, incoming_bytes):
                self._fsync_directory(path.parent)
                return str(path), range_digest
            existing_bytes: bytes | None = None
            merged = shares
            try:
                value = self._owned_lstat(path)
            except FileNotFoundError:
                value = None
            if value is not None:
                if not stat.S_ISREG(value.st_mode):
                    raise RuntimeError(
                        f"existing audit share segment is not regular at {path}"
                    )
                try:
                    existing_bytes, existing_descriptor = self._read_owned_regular_bytes(
                        path
                    )
                    if (
                        existing_descriptor.st_dev != value.st_dev
                        or existing_descriptor.st_ino != value.st_ino
                        or existing_descriptor.st_mtime_ns != value.st_mtime_ns
                    ):
                        raise OSError("share segment identity changed")
                    existing = json.loads(existing_bytes)
                except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
                    raise RuntimeError(
                        f"existing audit share segment is not valid JSON at {path}"
                    ) from exc
                if (
                    not isinstance(existing, dict)
                    or existing.get("schema") != AUDIT_SHARE_SEGMENT_SCHEMA
                    or not isinstance(existing.get("shares"), list)
                ):
                    raise RuntimeError(
                        f"existing audit share segment has invalid schema at {path}"
                    )
                merged = self.merge_audit_share_ranges(
                    existing["shares"],
                    shares,
                    segment_path=path,
                )
            segment = self.audit_share_segment_payload(
                first_share_seq=int(merged[0]["share_seq"]),
                last_share_seq=int(merged[-1]["share_seq"]),
                shares=merged,
            )
            if (
                int(merged[0]["share_seq"]) < segment_first_share_seq
                or int(merged[-1]["share_seq"]) > segment_last_share_seq
            ):
                raise RuntimeError("existing audit share range escapes its slot")
            segment_bytes = self.storage_json_bytes(segment)
            if existing_bytes != segment_bytes:
                self._write_mutable_bytes(path, segment_bytes)
            else:
                self._fsync_directory(path.parent)
        self._validate_root_identity()
        return str(path), range_digest

    @staticmethod
    def _validate_share_range(
        first_share_seq: int,
        last_share_seq: int,
        shares: list[Any],
    ) -> None:
        if first_share_seq <= 0 or last_share_seq < first_share_seq or not shares:
            raise RuntimeError("audit share range bounds are invalid")
        try:
            sequences = [int(share["share_seq"]) for share in shares]
        except (KeyError, TypeError, ValueError) as exc:
            raise RuntimeError("audit share range has invalid share_seq") from exc
        if (
            sequences[0] != first_share_seq
            or sequences[-1] != last_share_seq
            or any(current + 1 != nxt for current, nxt in zip(sequences, sequences[1:]))
        ):
            raise RuntimeError("audit share range is not exactly contiguous")

    def merge_audit_share_ranges(
        self,
        existing_shares: list[Any],
        incoming_shares: list[Any],
        *,
        segment_path: Path,
    ) -> list[Any]:
        if not existing_shares:
            return list(incoming_shares)
        existing = self.audit_shares_by_seq(existing_shares, segment_path=segment_path)
        incoming = self.audit_shares_by_seq(incoming_shares, segment_path=segment_path)
        for share_seq, value in incoming.items():
            old = existing.get(share_seq)
            if old is not None and old != value:
                raise RuntimeError(
                    "existing audit share segment conflicts at share_seq "
                    f"{share_seq} in {segment_path}"
                )
        merged = {**existing, **incoming}
        ordered = sorted(merged)
        if any(current + 1 != nxt for current, nxt in zip(ordered, ordered[1:])):
            raise RuntimeError(
                f"existing audit share segment would become non-contiguous at {segment_path}"
            )
        return [merged[share_seq] for share_seq in ordered]

    @staticmethod
    def audit_shares_by_seq(
        shares: list[Any],
        *,
        segment_path: Path,
    ) -> dict[int, Any]:
        result: dict[int, Any] = {}
        for share in shares:
            if not isinstance(share, dict):
                raise RuntimeError(
                    f"audit share segment has invalid share payload at {segment_path}"
                )
            try:
                share_seq = int(share["share_seq"])
            except (KeyError, TypeError, ValueError) as exc:
                raise RuntimeError(
                    f"audit share segment has invalid share_seq at {segment_path}"
                ) from exc
            old = result.get(share_seq)
            if old is not None and old != share:
                raise RuntimeError(
                    "audit share segment has duplicate conflicting share_seq "
                    f"{share_seq} at {segment_path}"
                )
            result[share_seq] = share
        ordered = sorted(result)
        if any(current + 1 != nxt for current, nxt in zip(ordered, ordered[1:])):
            raise RuntimeError(
                f"audit share segment has non-contiguous share_seq values at {segment_path}"
            )
        return result

    def audit_share_range_parts(self, shares: list[Any]) -> list[dict[str, Any]] | None:
        if self._share_segment_size <= 0:
            return None
        try:
            share_seqs = [int(share["share_seq"]) for share in shares]
        except (KeyError, TypeError, ValueError):
            return None
        if any(current + 1 != nxt for current, nxt in zip(share_seqs, share_seqs[1:])):
            return None
        parts: list[dict[str, Any]] = []
        index = 0
        while index < len(shares):
            first = share_seqs[index]
            segment_start = ((first - 1) // self._share_segment_size) * self._share_segment_size + 1
            segment_end = segment_start + self._share_segment_size - 1
            end = index
            while end < len(shares) and share_seqs[end] <= segment_end:
                end += 1
            chunk = shares[index:end]
            first_chunk = share_seqs[index]
            last_chunk = share_seqs[end - 1]
            uri, digest = self.write_audit_share_segment_range(
                segment_first_share_seq=segment_start,
                segment_last_share_seq=segment_end,
                first_share_seq=first_chunk,
                last_share_seq=last_chunk,
                shares=chunk,
            )
            parts.append(
                {
                    "kind": "segment_range",
                    "segment_first_share_seq": segment_start,
                    "segment_last_share_seq": segment_end,
                    "first_share_seq": first_chunk,
                    "last_share_seq": last_chunk,
                    "share_count": len(chunk),
                    "range_sha256": digest,
                    "body_uri": uri,
                }
            )
            index = end
        return parts

    def audit_share_parts(self, shares: list[Any]) -> list[dict[str, Any]] | None:
        if self._share_segment_size <= 0:
            return None
        try:
            share_seqs = [int(share["share_seq"]) for share in shares]
        except (KeyError, TypeError, ValueError):
            return None
        if any(current + 1 != nxt for current, nxt in zip(share_seqs, share_seqs[1:])):
            return None
        parts: list[dict[str, Any]] = []
        index = 0
        while index < len(shares):
            first = share_seqs[index]
            segment_start = ((first - 1) // self._share_segment_size) * self._share_segment_size + 1
            segment_end = segment_start + self._share_segment_size - 1
            end = index
            while end < len(shares) and share_seqs[end] <= segment_end:
                end += 1
            chunk = shares[index:end]
            chunk_seqs = share_seqs[index:end]
            if (
                len(chunk) == self._share_segment_size
                and chunk_seqs[0] == segment_start
                and chunk_seqs[-1] == segment_end
            ):
                uri, digest = self.write_audit_share_segment(
                    first_share_seq=segment_start,
                    last_share_seq=segment_end,
                    shares=chunk,
                )
                parts.append(
                    {
                        "kind": "segment",
                        "first_share_seq": segment_start,
                        "last_share_seq": segment_end,
                        "share_count": len(chunk),
                        "sha256": digest,
                        "body_uri": uri,
                    }
                )
            else:
                parts.append(
                    {
                        "kind": "inline",
                        "first_share_seq": chunk_seqs[0],
                        "last_share_seq": chunk_seqs[-1],
                        "share_count": len(chunk),
                        "shares": chunk,
                    }
                )
            index = end
        return parts

    def audit_body_ref(
        self,
        *,
        block_hash: str,
        audit_bundle_sha256: str,
        final_bundle: dict[str, Any],
    ) -> dict[str, Any] | None:
        if self._share_segment_size <= 0:
            return None
        shares = final_bundle.get("shares")
        if not isinstance(shares, list) or not shares:
            return None
        parts = self.audit_share_parts(shares)
        if parts is None or not any(part.get("kind") == "segment" for part in parts):
            return None
        without_shares = {key: value for key, value in final_bundle.items() if key != "shares"}
        return {
            "schema": AUDIT_BODY_REF_SCHEMA,
            "block_hash": block_hash,
            "audit_bundle_sha256": audit_bundle_sha256,
            "audit_bundle_schema": str(final_bundle.get("schema") or ""),
            "share_count": len(shares),
            "share_segment_size": self._share_segment_size,
            "shares_key_index": list(final_bundle).index("shares"),
            "bundle_without_shares": without_shares,
            "share_parts": parts,
        }

    def audit_bundle_v2(
        self,
        *,
        block_hash: str,
        audit_bundle_sha256: str,
        final_bundle: dict[str, Any],
    ) -> dict[str, Any] | None:
        if self._share_segment_size <= 0:
            return None
        shares = final_bundle.get("shares")
        if not isinstance(shares, list) or not shares:
            return None
        parts = self.audit_share_range_parts(shares)
        if parts is None:
            return None
        proof: dict[str, Any] = {
            "schema": AUDIT_WINDOW_COMPLETENESS_PROOF_SCHEMA,
            "share_segment_size": self._share_segment_size,
            "first_share_seq": int(shares[0]["share_seq"]),
            "last_share_seq": int(shares[-1]["share_seq"]),
            "share_count": len(shares),
            "share_parts_digest_hex": _sha256_bytes(
                self.storage_json_bytes({"share_parts": parts})
            ),
            "share_parts": parts,
        }
        reward = final_bundle.get("reward_manifest")
        if isinstance(reward, dict):
            for key in (
                "anchor_job_issued_at_ms",
                "anchor_share_seq",
                "newest_share_seq",
                "oldest_share_seq",
                "included_share_count",
                "requested_window_weight",
                "counted_window_weight",
                "share_slice_digest_hex",
            ):
                if key in reward:
                    proof[key] = reward[key]
        return {
            "schema": AUDIT_BUNDLE_V2_SCHEMA,
            "block_hash": block_hash,
            "audit_bundle_sha256": audit_bundle_sha256,
            "logical_audit_bundle_schema": str(final_bundle.get("schema") or ""),
            "share_count": len(shares),
            "shares_key_index": list(final_bundle).index("shares"),
            "bundle_without_shares": {
                key: value for key, value in final_bundle.items() if key != "shares"
            },
            "share_window_proof": proof,
        }

    def externalize_audit_body(
        self,
        block_hash: str,
        audit_bundle_sha256: str,
        final_bundle: dict[str, Any],
    ) -> str:
        block_hash = _canonical_hex(block_hash, name="block_hash")
        digest = _canonical_hex(
            audit_bundle_sha256,
            name="audit_bundle_sha256",
        )
        body = self.canonical_audit_body_bytes_for_sha(final_bundle, digest)
        path = self.body_path(block_hash, digest)
        self._write_immutable_bytes(path, body)
        self._validate_root_identity()
        return str(path)

    def prepare_external_audit_body(
        self,
        payload: Mapping[str, Any],
        final_bundle: dict[str, Any],
        *,
        body_uri: str | None,
        canonical_bundle_path: Path | None = None,
    ) -> str | None:
        block_hash = _canonical_hex(payload["block_hash"], name="block_hash")
        expected = _canonical_hex(
            payload["audit_bundle_sha256"],
            name="audit_bundle_sha256",
        )
        if body_uri is None:
            return None
        body_path = self.resolve_owned_path(body_uri)
        canonical_path = self.body_path(block_hash, expected)
        if body_path != canonical_path:
            raise RuntimeError(
                "existing audit bundle body pointer does not match canonical external path: "
                f"{body_uri}"
            )
        literal: bytes | None = None
        source_identity: _FileIdentity | None = None
        if canonical_bundle_path is not None:
            source = Path(canonical_bundle_path)
            try:
                literal, before = self._read_owned_regular_bytes(source)
            except OSError as exc:
                raise RuntimeError(
                    f"canonical audit bundle is not retrievable at {source}: {exc}"
                ) from exc
            source_identity = _FileIdentity.from_stat(before)
            actual = _sha256_bytes(literal)
            if not hmac.compare_digest(actual, expected):
                raise RuntimeError(
                    f"audit bundle sha256 mismatch: expected {expected}, got {actual}"
                )
        # Compact storage is derived from final_bundle, not necessarily from the
        # supplied canonical path. Bind those logical inputs independently.
        if literal is not None:
            try:
                canonical_logical = json.loads(literal)
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise RuntimeError("canonical audit candidate is not valid JSON") from exc
            if canonical_logical != final_bundle:
                raise RuntimeError(
                    "canonical audit candidate does not match logical bundle"
                )
        else:
            self.canonical_audit_body_bytes_for_sha(final_bundle, expected)
        storage = self.audit_bundle_v2(
            block_hash=block_hash,
            audit_bundle_sha256=expected,
            final_bundle=final_bundle,
        )
        if storage is None:
            storage = self.audit_body_ref(
                block_hash=block_hash,
                audit_bundle_sha256=expected,
                final_bundle=final_bundle,
            )
        if storage is not None:
            body_bytes = self.storage_json_bytes(storage)
        elif literal is not None:
            body_bytes = literal
        else:
            body_bytes = self.canonical_audit_body_bytes_for_sha(
                final_bundle,
                expected,
            )
        try:
            existing = self._owned_lstat(body_path)
        except FileNotFoundError:
            existing = None
        if existing is not None:
            if not stat.S_ISREG(existing.st_mode):
                raise RuntimeError(
                    f"existing audit bundle body is not regular at {body_path}"
                )
            if storage is not None and self.file_matches_bytes(body_path, body_bytes):
                if self._compact_body_reconstructs_to(
                    body_path,
                    expected=expected,
                    final_bundle=final_bundle,
                ):
                    self._fsync_directory(body_path.parent)
                    return str(body_path)
                raise RuntimeError(
                    f"existing audit bundle body does not match payload at {body_path}"
                )
            if storage is None and self.external_body_matches_sha(body_path, expected):
                self._fsync_directory(body_path.parent)
                return str(body_path)
            # Layout upgrades may represent the same canonical logical bundle
            # with different storage bytes.  Verify by reconstruction.
            if not self.external_body_matches_sha(body_path, expected):
                raise RuntimeError(
                    f"existing audit bundle body does not match payload at {body_path}"
                )
            self._fsync_directory(body_path.parent)
            return str(body_path)
        self._write_immutable_bytes(body_path, body_bytes)
        if canonical_bundle_path is not None and source_identity is not None:
            source_after = self._owned_lstat(Path(canonical_bundle_path))
            if not source_identity.matches(source_after):
                raise RuntimeError("canonical audit bundle identity changed after publication")
        # The compact storage bytes were derived from the already verified
        # logical bundle.  Validate exact destination bytes here; expensive
        # reconstruction remains only the cross-version mismatch path.
        if storage is not None:
            valid = self._compact_body_reconstructs_to(
                body_path,
                expected=expected,
                final_bundle=final_bundle,
            )
        else:
            valid = self.external_body_matches_sha(body_path, expected)
        if not valid:
            raise RuntimeError("published audit bundle body failed digest verification")
        self._validate_root_identity()
        return str(body_path)

    def _compact_body_reconstructs_to(
        self,
        body_path: Path,
        *,
        expected: str,
        final_bundle: Mapping[str, Any],
    ) -> bool:
        try:
            body_bytes, _value = self._read_owned_regular_bytes(body_path)
            body = json.loads(body_bytes)
            if not isinstance(body, dict):
                return False
            if body.get("schema") == AUDIT_BODY_REF_SCHEMA:
                reconstructed = self.resolve_audit_body_ref(
                    body,
                    expected_sha256=expected,
                    body_uri=str(body_path),
                    verify_digest=False,
                )
            elif body.get("schema") == AUDIT_BUNDLE_V2_SCHEMA:
                reconstructed = self.resolve_audit_bundle_v2(
                    body,
                    expected_sha256=expected,
                    body_uri=str(body_path),
                    verify_digest=False,
                )
            else:
                return False
            return reconstructed == final_bundle
        except (
            OSError,
            RuntimeError,
            TypeError,
            ValueError,
            UnicodeDecodeError,
            json.JSONDecodeError,
        ):
            return False

    def validate_canonical_source(
        self,
        path: Path,
        expected_sha256: str,
        final_bundle: Mapping[str, Any] | None = None,
    ) -> None:
        expected = _canonical_hex(
            expected_sha256,
            name="audit_bundle_sha256",
        )
        try:
            payload, _value = self._read_owned_regular_bytes(Path(path))
        except OSError as exc:
            raise RuntimeError(
                f"canonical audit bundle is not retrievable at {path}: {exc}"
            ) from exc
        actual = _sha256_bytes(payload)
        if not hmac.compare_digest(actual, expected):
            raise RuntimeError(
                f"audit bundle sha256 mismatch: expected {expected}, got {actual}"
            )
        if final_bundle is not None:
            try:
                logical = json.loads(payload)
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise RuntimeError("canonical audit candidate is not valid JSON") from exc
            if logical != final_bundle:
                raise RuntimeError(
                    "canonical audit candidate does not match logical bundle"
                )
        self._validate_root_identity()

    def audit_body_byte_len(
        self,
        body_uri: object | None,
        final_bundle: dict[str, Any],
        canonical_bundle_path: Path | None = None,
    ) -> int:
        if body_uri:
            _payload, value = self._read_owned_regular_bytes(
                self.resolve_owned_path(body_uri)
            )
            self._validate_root_identity()
            return value.st_size
        if canonical_bundle_path is not None:
            _payload, value = self._read_owned_regular_bytes(Path(canonical_bundle_path))
            self._validate_root_identity()
            return value.st_size
        value = len(self.canonical_audit_bundle_bytes(final_bundle))
        self._validate_root_identity()
        return value

    def read_external_body(
        self,
        body_uri: object,
        *,
        expected_sha256: object | None = None,
    ) -> dict[str, object] | None:
        if not body_uri:
            return None
        try:
            path = self.resolve_owned_path(body_uri)
            body_match = _BODY_RE.fullmatch(path.name)
            if body_match is None:
                raise RuntimeError("body URI is not an owned canonical body")
            if expected_sha256 is not None and body_match.group("digest") != _canonical_hex(
                expected_sha256,
                name="audit_bundle_sha256",
            ):
                raise RuntimeError("body URI digest does not match expected digest")
            body_bytes, _value = self._read_owned_regular_bytes(path)
        except (OSError, RuntimeError, TypeError, ValueError, OverflowError) as exc:
            raise RuntimeError(
                f"audit bundle body is not retrievable at {body_uri}: {exc}"
            ) from exc
        try:
            body = json.loads(body_bytes.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise RuntimeError(
                f"audit bundle body is not valid JSON at {body_uri}: {exc}"
            ) from exc
        try:
            if isinstance(body, dict) and body.get("schema") == AUDIT_BODY_REF_SCHEMA:
                resolved = self.resolve_audit_body_ref(
                    body,
                    expected_sha256=expected_sha256,
                    body_uri=body_uri,
                )
                self._validate_root_identity()
                return resolved
            if isinstance(body, dict) and body.get("schema") == AUDIT_BUNDLE_V2_SCHEMA:
                resolved = self.resolve_audit_bundle_v2(
                    body,
                    expected_sha256=expected_sha256,
                    body_uri=body_uri,
                )
                self._validate_root_identity()
                return resolved
            if expected_sha256:
                expected = _canonical_hex(
                    expected_sha256,
                    name="audit_bundle_sha256",
                )
                actual = _sha256_bytes(body_bytes)
                if not hmac.compare_digest(actual, expected):
                    raise RuntimeError(
                        f"audit bundle body hash mismatch at {body_uri}: "
                        f"expected {expected}, got {actual}"
                    )
            self._validate_root_identity()
            return body
        except RuntimeError:
            raise
        except (TypeError, ValueError, OverflowError) as exc:
            raise RuntimeError(
                f"audit bundle body is not valid JSON at {body_uri}: {exc}"
            ) from exc

    def external_body_matches_sha(self, body_path: Path, expected: str) -> bool:
        try:
            self.read_external_body(str(body_path), expected_sha256=expected)
        except (
            OSError,
            RuntimeError,
            TypeError,
            ValueError,
            OverflowError,
            UnicodeDecodeError,
            json.JSONDecodeError,
        ):
            return False
        return True

    def external_body_available_for_sha(self, body_uri: object, expected: str) -> bool:
        try:
            expected = _canonical_hex(expected, name="audit_bundle_sha256")
            self.read_external_body(body_uri, expected_sha256=expected)
        except (
            OSError,
            RuntimeError,
            TypeError,
            ValueError,
            OverflowError,
            UnicodeDecodeError,
            json.JSONDecodeError,
        ):
            return False
        return True

    def resolve_audit_body_ref(
        self,
        body_ref: Mapping[str, Any],
        *,
        expected_sha256: object | None,
        body_uri: object,
        verify_digest: bool = True,
    ) -> dict[str, object]:
        expected = (
            _canonical_hex(expected_sha256, name="audit_bundle_sha256")
            if expected_sha256
            else None
        )
        declared = _canonical_hex(
            body_ref.get("audit_bundle_sha256"),
            name="audit_bundle_sha256",
        )
        self._validate_body_wrapper_identity(body_ref, body_uri, declared)
        if expected and not hmac.compare_digest(declared, expected):
            raise RuntimeError(
                f"audit bundle body hash mismatch at {body_uri}: expected {expected}, got {declared}"
            )
        without_shares = body_ref.get("bundle_without_shares")
        parts = body_ref.get("share_parts")
        if not isinstance(without_shares, dict) or not isinstance(parts, list):
            raise RuntimeError(
                f"audit bundle body is not valid JSON at {body_uri}: invalid body reference"
            )
        shares: list[Any] = []
        previous_last_share_seq: int | None = None
        for part in parts:
            if not isinstance(part, dict):
                raise RuntimeError(
                    f"audit bundle body is not valid JSON at {body_uri}: invalid share part"
                )
            kind = part.get("kind")
            if kind in {"segment", "segment_range", "segment_prefix"}:
                part_shares = self.read_audit_share_segment(
                    part,
                    parent_body_uri=body_uri,
                )
            elif kind == "inline" and isinstance(part.get("shares"), list):
                inline = part["shares"]
                if len(inline) != int(part.get("share_count") or 0):
                    raise RuntimeError(
                        f"audit bundle body is not valid JSON at {body_uri}: inline share count mismatch"
                    )
                self._validate_share_range(
                    int(part.get("first_share_seq") or 0),
                    int(part.get("last_share_seq") or 0),
                    inline,
                )
                part_shares = inline
            else:
                raise RuntimeError(
                    f"audit bundle body is not valid JSON at {body_uri}: invalid share part kind"
                )
            first_share_seq = int(part.get("first_share_seq") or 0)
            last_share_seq = int(part.get("last_share_seq") or 0)
            if (
                previous_last_share_seq is not None
                and first_share_seq <= previous_last_share_seq
            ):
                raise RuntimeError(
                    f"audit bundle body is not valid JSON at {body_uri}: share parts overlap or are out of order"
                )
            previous_last_share_seq = last_share_seq
            shares.extend(part_shares)
        if len(shares) != int(body_ref.get("share_count") or 0):
            raise RuntimeError(
                f"audit bundle body is not valid JSON at {body_uri}: share count mismatch"
            )
        bundle = self._insert_shares(
            without_shares,
            shares,
            int(body_ref.get("shares_key_index", len(without_shares))),
        )
        if verify_digest:
            actual = _sha256_bytes(self.canonical_audit_bundle_bytes(bundle))
            if not hmac.compare_digest(actual, declared):
                raise RuntimeError(
                    f"audit bundle body hash mismatch at {body_uri}: expected {declared}, got {actual}"
                )
        return bundle

    def resolve_audit_bundle_v2(
        self,
        body: Mapping[str, Any],
        *,
        expected_sha256: object | None,
        body_uri: object,
        verify_digest: bool = True,
    ) -> dict[str, object]:
        expected = (
            _canonical_hex(expected_sha256, name="audit_bundle_sha256")
            if expected_sha256
            else None
        )
        declared = _canonical_hex(
            body.get("audit_bundle_sha256"),
            name="audit_bundle_sha256",
        )
        self._validate_body_wrapper_identity(body, body_uri, declared)
        if expected and not hmac.compare_digest(declared, expected):
            raise RuntimeError(
                f"audit bundle body hash mismatch at {body_uri}: expected {expected}, got {declared}"
            )
        without_shares = body.get("bundle_without_shares")
        proof = body.get("share_window_proof")
        if not isinstance(without_shares, dict) or not isinstance(proof, dict):
            raise RuntimeError(
                f"audit bundle body is not valid JSON at {body_uri}: invalid v2 body"
            )
        if proof.get("schema") != AUDIT_WINDOW_COMPLETENESS_PROOF_SCHEMA:
            raise RuntimeError(
                f"audit bundle body is not valid JSON at {body_uri}: invalid proof schema"
            )
        if int(body.get("share_count") or 0) != int(proof.get("share_count") or 0):
            raise RuntimeError(
                f"audit bundle body is not valid JSON at {body_uri}: proof share count mismatch"
            )
        parts = proof.get("share_parts")
        if not isinstance(parts, list):
            raise RuntimeError(
                f"audit bundle body is not valid JSON at {body_uri}: missing share parts"
            )
        expected_parts_digest = str(proof.get("share_parts_digest_hex") or "").lower()
        actual_parts_digest = _sha256_bytes(
            self.storage_json_bytes({"share_parts": parts})
        )
        if expected_parts_digest != actual_parts_digest:
            raise RuntimeError(
                f"audit bundle body is not valid JSON at {body_uri}: share parts digest mismatch"
            )
        shares: list[Any] = []
        for part in parts:
            if not isinstance(part, dict):
                raise RuntimeError(
                    f"audit bundle body is not valid JSON at {body_uri}: invalid share part"
                )
            shares.extend(self.read_audit_share_segment(part, parent_body_uri=body_uri))
        if len(shares) != int(proof.get("share_count") or 0):
            raise RuntimeError(
                f"audit bundle body is not valid JSON at {body_uri}: share count mismatch"
            )
        if shares and (
            int(shares[0].get("share_seq") or 0)
            != int(proof.get("first_share_seq") or 0)
            or int(shares[-1].get("share_seq") or 0)
            != int(proof.get("last_share_seq") or 0)
        ):
            raise RuntimeError(
                f"audit bundle body is not valid JSON at {body_uri}: proof range mismatch"
            )
        bundle = self._insert_shares(
            without_shares,
            shares,
            int(body.get("shares_key_index", len(without_shares))),
        )
        reward_manifest = bundle.get("reward_manifest")
        copied_proof_fields = (
            "anchor_job_issued_at_ms",
            "anchor_share_seq",
            "newest_share_seq",
            "oldest_share_seq",
            "included_share_count",
            "requested_window_weight",
            "counted_window_weight",
            "share_slice_digest_hex",
        )
        if isinstance(reward_manifest, dict):
            for field in copied_proof_fields:
                if (field in proof) != (field in reward_manifest) or proof.get(
                    field
                ) != reward_manifest.get(field):
                    raise RuntimeError(
                        f"audit bundle body is not valid JSON at {body_uri}: proof {field} mismatch"
                    )
        elif any(field in proof for field in copied_proof_fields):
            raise RuntimeError(
                f"audit bundle body is not valid JSON at {body_uri}: proof has no reward manifest"
            )
        if verify_digest:
            actual = _sha256_bytes(self.canonical_audit_bundle_bytes(bundle))
            if not hmac.compare_digest(actual, declared):
                raise RuntimeError(
                    f"audit bundle body hash mismatch at {body_uri}: expected {declared}, got {actual}"
                )
        return bundle

    def _validate_body_wrapper_identity(
        self,
        body: Mapping[str, Any],
        body_uri: object,
        declared_digest: str,
    ) -> None:
        path = self.resolve_owned_path(body_uri)
        match = _BODY_RE.fullmatch(path.name)
        if match is None:
            raise RuntimeError("compact audit body path is invalid")
        block_hash = _canonical_hex(body.get("block_hash"), name="block_hash")
        if (
            block_hash != match.group("block")
            or declared_digest != match.group("digest")
        ):
            raise RuntimeError("compact audit body identity mismatch")

    @staticmethod
    def _insert_shares(
        without_shares: Mapping[str, Any],
        shares: list[Any],
        shares_key_index: int,
    ) -> dict[str, object]:
        bundle: dict[str, object] = {}
        inserted = False
        for index, (key, value) in enumerate(without_shares.items()):
            if index == shares_key_index:
                bundle["shares"] = shares
                inserted = True
            bundle[str(key)] = value
        if not inserted:
            bundle["shares"] = shares
        return bundle

    def read_audit_share_segment(
        self,
        part: Mapping[str, Any],
        *,
        parent_body_uri: object,
    ) -> list[Any]:
        body_uri = part.get("body_uri")
        kind = str(part.get("kind") or "")
        if kind not in {"segment", "segment_range", "segment_prefix"}:
            raise RuntimeError(
                f"audit bundle body is not valid JSON at {parent_body_uri}: "
                "invalid share part kind"
            )
        try:
            path = self.resolve_owned_path(body_uri)
            if self.artifact_kind(path.name) != "share_segment":
                raise RuntimeError("not an owned share segment")
            if kind == "segment":
                match = _SHARE_CONTENT_RE.fullmatch(path.name)
                if match is None:
                    raise RuntimeError("immutable share segment path is invalid")
                if (
                    int(match.group("first")) != int(part.get("first_share_seq") or 0)
                    or int(match.group("last")) != int(part.get("last_share_seq") or 0)
                ):
                    raise RuntimeError("immutable share segment path bounds mismatch")
            elif kind in {"segment_range", "segment_prefix"}:
                match = _SHARE_SLOT_RE.fullmatch(path.name)
                if match is None:
                    raise RuntimeError("share segment slot path is invalid")
                declared_slot_first = int(
                    part.get("segment_first_share_seq")
                    or match.group("first")
                )
                declared_slot_last = int(
                    part.get("segment_last_share_seq")
                    or match.group("last")
                )
                if (
                    declared_slot_first != int(match.group("first"))
                    or declared_slot_last != int(match.group("last"))
                ):
                    raise RuntimeError("share segment slot path bounds mismatch")
            segment_bytes, _value = self._read_owned_regular_bytes(path)
        except (OSError, RuntimeError) as exc:
            raise RuntimeError(
                f"audit bundle body is not retrievable at {parent_body_uri}: "
                f"share segment {body_uri}: {exc}"
            ) from exc
        if kind == "segment":
            expected = str(part.get("sha256") or "").lower()
            actual = _sha256_bytes(segment_bytes)
            if not hmac.compare_digest(actual, expected):
                raise RuntimeError(
                    f"audit bundle body hash mismatch at {parent_body_uri}: "
                    f"share segment {body_uri} expected {expected}, got {actual}"
                )
        try:
            segment = json.loads(segment_bytes)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise RuntimeError(
                f"audit bundle body is not valid JSON at {parent_body_uri}: "
                f"share segment {body_uri}: {exc}"
            ) from exc
        if (
            not isinstance(segment, dict)
            or segment.get("schema") != AUDIT_SHARE_SEGMENT_SCHEMA
            or not isinstance(segment.get("shares"), list)
        ):
            raise RuntimeError(
                f"audit bundle body is not valid JSON at {parent_body_uri}: "
                f"invalid share segment {body_uri}"
            )
        segment_shares = segment["shares"]
        try:
            segment_first = int(segment.get("first_share_seq"))
            segment_last = int(segment.get("last_share_seq"))
            segment_count = int(segment.get("share_count"))
            self._validate_share_range(
                segment_first,
                segment_last,
                segment_shares,
            )
        except (TypeError, ValueError, RuntimeError) as exc:
            raise RuntimeError(
                f"audit bundle body is not valid JSON at {parent_body_uri}: "
                f"share segment {body_uri} header mismatch"
            ) from exc
        if segment_count != len(segment_shares):
            raise RuntimeError(
                f"audit bundle body is not valid JSON at {parent_body_uri}: "
                f"share segment {body_uri} share count mismatch"
            )
        if kind == "segment":
            assert match is not None
            if (
                segment_first != int(match.group("first"))
                or segment_last != int(match.group("last"))
            ):
                raise RuntimeError("immutable share segment header bounds mismatch")
        else:
            assert match is not None
            if (
                segment_first < int(match.group("first"))
                or segment_last > int(match.group("last"))
            ):
                raise RuntimeError("share segment header escapes slot bounds")
        selected = self.select_audit_share_segment_range(
            segment_shares,
            first_share_seq=int(part.get("first_share_seq") or 0),
            last_share_seq=int(part.get("last_share_seq") or 0),
            parent_body_uri=parent_body_uri,
            body_uri=body_uri,
        )
        if len(selected) != int(part.get("share_count") or 0):
            raise RuntimeError(
                f"audit bundle body is not valid JSON at {parent_body_uri}: "
                f"share segment {body_uri} share count mismatch"
            )
        if kind in {"segment_range", "segment_prefix"}:
            key = "range_sha256" if kind == "segment_range" else "prefix_sha256"
            actual = _sha256_bytes(
                self.storage_json_bytes(
                    self.audit_share_segment_payload(
                        first_share_seq=int(part.get("first_share_seq") or 0),
                        last_share_seq=int(part.get("last_share_seq") or 0),
                        shares=selected,
                    )
                )
            )
            expected = str(part.get(key) or "").lower()
            if not hmac.compare_digest(actual, expected):
                raise RuntimeError(
                    f"audit bundle body hash mismatch at {parent_body_uri}: "
                    f"share segment range {body_uri} expected {expected}, got {actual}"
                )
        elif kind != "segment":
            raise RuntimeError(
                f"audit bundle body is not valid JSON at {parent_body_uri}: invalid share part kind"
            )
        self._validate_root_identity()
        return selected

    @staticmethod
    def select_audit_share_segment_range(
        shares: list[Any],
        *,
        first_share_seq: int,
        last_share_seq: int,
        parent_body_uri: object,
        body_uri: object,
    ) -> list[Any]:
        selected: list[Any] = []
        previous: int | None = None
        for share in shares:
            if not isinstance(share, dict):
                raise RuntimeError(
                    f"audit bundle body is not valid JSON at {parent_body_uri}: "
                    f"share segment {body_uri} has invalid share"
                )
            share_seq = int(share.get("share_seq") or 0)
            if previous is not None and previous + 1 != share_seq:
                raise RuntimeError(
                    f"audit bundle body is not valid JSON at {parent_body_uri}: "
                    f"share segment {body_uri} is not contiguous"
                )
            previous = share_seq
            if first_share_seq <= share_seq <= last_share_seq:
                selected.append(share)
        if (
            not selected
            or int(selected[0].get("share_seq") or 0) != first_share_seq
            or int(selected[-1].get("share_seq") or 0) != last_share_seq
        ):
            raise RuntimeError(
                f"audit bundle body is not valid JSON at {parent_body_uri}: "
                f"share segment {body_uri} does not contain requested range"
            )
        return selected


def _canonical_hex_bytes(value: object, *, name: str) -> str:
    text = str(value)
    if len(text) % 2:
        raise ValueError(f"{name} must contain complete hexadecimal bytes")
    try:
        bytes.fromhex(text)
    except ValueError as exc:
        raise ValueError(f"{name} must be hexadecimal") from exc
    return text.lower()
