from abc import ABC, abstractmethod

import lmdb


class BaseBlockStore(ABC):
    """Store interface for block blobs"""

    @abstractmethod
    def add_block(self, block_hash: bytes, block_blob: bytes) -> None:
        pass

    @abstractmethod
    def get_block(self, block_hash: bytes) -> bytes:
        pass


class LMDBLockStore(BaseBlockStore):
    """BlockStore implementation based on LMBD"""

    def __init__(self, block_dir: str) -> None:
        # Change the directory
        self.env = lmdb.open(block_dir, subdir=True)
        # add sub dbs if required

    def add_block(self, block_hash: bytes, block_blob: bytes) -> None:
        with self.env.begin(write=True) as txn:
            txn.put(block_hash, block_blob)

    def get_block(self, block_hash: bytes) -> bytes:
        with self.env.begin() as txn:
            val = txn.get(block_hash)
        return val

    def close(self) -> None:
        self.env.close()