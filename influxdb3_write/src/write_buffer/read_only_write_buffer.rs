//! Read-only write buffer for replica nodes: no WAL/Persister, chunks from TableIndexCache only.

use std::sync::Arc;

use async_trait::async_trait;
use data_types::NamespaceName;
use datafusion::{
    catalog::Session,
    common::DataFusionError,
    datasource::object_store::ObjectStoreUrl,
};
use influxdb3_cache::{distinct_cache::DistinctCacheProvider, last_cache::LastCacheProvider};
use influxdb3_catalog::catalog::{Catalog, DatabaseSchema, TableDefinition};
use influxdb3_id::{DbId, TableId};
use influxdb3_wal::{
    SnapshotDetails, SnapshotSequenceNumber, Wal, WalFileSequenceNumber, WalOp,
};
use iox_query::QueryChunk;
use object_store::ObjectStore;
use observability_deps::tracing::warn;
use tokio::sync::oneshot;

use crate::{
    BufferedWriteRequest, Bufferer, ChunkContainer, ChunkFilter, DistinctCacheManager, LastCacheManager,
    ParquetFile, PersistedSnapshotVersion, Precision, WriteBuffer,
    table_index_cache::{TableIndexCache, TableIndexCacheError},
};
use super::parquet_chunk_from_file;

/// No-op WAL for read-only replicas (no writes).
#[derive(Debug, Clone, Copy)]
pub struct NoOpWal;

#[async_trait]
impl Wal for NoOpWal {
    async fn write_ops_unconfirmed(&self, _op: Vec<WalOp>) -> influxdb3_wal::Result<(), influxdb3_wal::Error> {
        Ok(())
    }

    async fn write_ops(&self, _ops: Vec<WalOp>) -> influxdb3_wal::Result<(), influxdb3_wal::Error> {
        Ok(())
    }

    async fn flush_buffer(
        &self,
    ) -> Option<(
        oneshot::Receiver<SnapshotDetails>,
        SnapshotDetails,
        tokio::sync::OwnedSemaphorePermit,
    )> {
        None
    }

    async fn force_flush_buffer(
        &self,
    ) -> Option<(
        oneshot::Receiver<SnapshotDetails>,
        SnapshotDetails,
        tokio::sync::OwnedSemaphorePermit,
    )> {
        None
    }

    async fn cleanup_snapshot(
        &self,
        _snapshot_details: SnapshotDetails,
        _snapshot_permit: tokio::sync::OwnedSemaphorePermit,
    ) {
    }

    async fn last_wal_sequence_number(&self) -> WalFileSequenceNumber {
        WalFileSequenceNumber::new(0)
    }

    async fn last_snapshot_sequence_number(&self) -> SnapshotSequenceNumber {
        SnapshotSequenceNumber::new(0)
    }

    async fn shutdown(&self) {}

    fn add_file_notifier(&self, _notifier: Arc<dyn influxdb3_wal::WalFileNotifier>) {}
}

/// Read-only implementation of WriteBuffer: no WAL/Persister, get_table_chunks from TableIndexCache.
#[derive(Debug)]
pub struct ReadOnlyWriteBuffer {
    catalog: Arc<Catalog>,
    table_index_cache: Arc<TableIndexCache>,
    data_path_prefix: String,
    object_store: Arc<dyn ObjectStore>,
    object_store_url: ObjectStoreUrl,
    last_cache: Arc<LastCacheProvider>,
    distinct_cache: Arc<DistinctCacheProvider>,
    query_file_limit: usize,
    wal: Arc<NoOpWal>,
    watch_rx: tokio::sync::watch::Receiver<Option<PersistedSnapshotVersion>>,
}

impl ReadOnlyWriteBuffer {
    pub fn new(
        catalog: Arc<Catalog>,
        table_index_cache: Arc<TableIndexCache>,
        data_path_prefix: String,
        object_store: Arc<dyn ObjectStore>,
        object_store_url: ObjectStoreUrl,
        last_cache: Arc<LastCacheProvider>,
        distinct_cache: Arc<DistinctCacheProvider>,
        query_file_limit: usize,
    ) -> Self {
        let (_tx, watch_rx) = tokio::sync::watch::channel(None);
        Self {
            catalog,
            table_index_cache,
            data_path_prefix,
            object_store,
            object_store_url,
            last_cache,
            distinct_cache,
            query_file_limit,
            wal: Arc::new(NoOpWal),
            watch_rx,
        }
    }
}

#[async_trait]
impl Bufferer for ReadOnlyWriteBuffer {
    async fn write_lp(
        &self,
        _database: NamespaceName<'static>,
        _lp: &str,
        _ingest_time: iox_time::Time,
        _accept_partial: bool,
        _precision: Precision,
        _no_sync: bool,
    ) -> crate::write_buffer::Result<BufferedWriteRequest> {
        Err(super::Error::ReadOnlyReplica)
    }

    fn catalog(&self) -> Arc<Catalog> {
        Arc::clone(&self.catalog)
    }

    fn wal(&self) -> Arc<dyn Wal> {
        Arc::clone(&self.wal) as Arc<dyn Wal>
    }

    fn parquet_files(&self, _db_id: DbId, _table_id: TableId) -> Vec<ParquetFile> {
        self.parquet_files_filtered(_db_id, _table_id, &ChunkFilter::default())
    }

    fn parquet_files_filtered(
        &self,
        db_id: DbId,
        table_id: TableId,
        filter: &ChunkFilter<'_>,
    ) -> Vec<ParquetFile> {
        let table_index_id = influxdb3_id::TableIndexId::new(
            self.data_path_prefix.as_str(),
            db_id,
            table_id,
        );
        let index = match tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current()
                .block_on(self.table_index_cache.get_or_load(&table_index_id))
        }) {
            Ok(idx) => idx,
            Err(TableIndexCacheError::CreateTableIndexFromObjectStoreError(
                crate::table_index::TableIndexError::NotFound,
            ))
            | Err(TableIndexCacheError::LoadTableIndexFromObjectStoreError(
                crate::table_index::TableIndexError::NotFound,
            )) => return vec![],
            Err(e) => {
                warn!(%table_index_id, error = %e, "read-only: failed to load table index");
                return vec![];
            }
        };
        let mut boxed = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(index.parquet_files())
        });
        let mut files = Vec::new();
        let it = &mut *boxed;
        while let Some(f) = std::iter::Iterator::next(it) {
            if filter.test_time_stamp_min_max(f.min_time, f.max_time) {
                files.push((*f).clone());
            }
        }
        files
    }

    fn watch_persisted_snapshots(
        &self,
    ) -> tokio::sync::watch::Receiver<Option<PersistedSnapshotVersion>> {
        self.watch_rx.clone()
    }
}

impl ChunkContainer for ReadOnlyWriteBuffer {
    fn get_table_chunks(
        &self,
        db_schema: Arc<DatabaseSchema>,
        table_def: Arc<TableDefinition>,
        filter: &ChunkFilter<'_>,
        projection: Option<&Vec<usize>>,
        ctx: &dyn Session,
    ) -> Result<Vec<Arc<dyn QueryChunk>>, DataFusionError> {
        let _ = (projection, ctx);
        let table_index_id = influxdb3_id::TableIndexId::new(
            self.data_path_prefix.as_str(),
            db_schema.id,
            table_def.table_id,
        );
        let index = match tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current()
                .block_on(self.table_index_cache.get_or_load(&table_index_id))
        }) {
            Ok(idx) => idx,
            Err(TableIndexCacheError::CreateTableIndexFromObjectStoreError(
                crate::table_index::TableIndexError::NotFound,
            ))
            | Err(TableIndexCacheError::LoadTableIndexFromObjectStoreError(
                crate::table_index::TableIndexError::NotFound,
            )) => return Ok(vec![]),
            Err(e) => {
                return Err(DataFusionError::External(
                    format!("read-only: failed to load table index {}: {}", table_index_id, e).into(),
                ));
            }
        };
        let mut boxed = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(index.parquet_files())
        });
        let mut parquet_files = Vec::new();
        let it = &mut *boxed;
        while let Some(f) = std::iter::Iterator::next(it) {
            if filter.test_time_stamp_min_max(f.min_time, f.max_time) {
                parquet_files.push((*f).clone());
            }
        }
        if parquet_files.len() > self.query_file_limit {
            return Err(DataFusionError::External(
                format!(
                    "Query would scan {} Parquet files, exceeding the file limit ({}).",
                    parquet_files.len(),
                    self.query_file_limit
                )
                .into(),
            ));
        }
        let mut chunks: Vec<Arc<dyn QueryChunk>> = Vec::with_capacity(parquet_files.len());
        for (chunk_order, parquet_file) in parquet_files.into_iter().enumerate() {
            let parquet_chunk = parquet_chunk_from_file(
                &parquet_file,
                &table_def.schema,
                self.object_store_url.clone(),
                Arc::clone(&self.object_store),
                chunk_order as i64,
            );
            chunks.push(Arc::new(parquet_chunk));
        }
        Ok(chunks)
    }
}

#[async_trait::async_trait]
impl DistinctCacheManager for ReadOnlyWriteBuffer {
    fn distinct_cache_provider(&self) -> Arc<DistinctCacheProvider> {
        Arc::clone(&self.distinct_cache)
    }
}

#[async_trait::async_trait]
impl LastCacheManager for ReadOnlyWriteBuffer {
    fn last_cache_provider(&self) -> Arc<LastCacheProvider> {
        Arc::clone(&self.last_cache)
    }
}

impl WriteBuffer for ReadOnlyWriteBuffer {}
