// Copyright (c) 2025 Rust Nostr Developers
// Distributed under the MIT software license

use heed::EnvFlags;

/// LMDB configuration options
#[derive(Debug, Clone)]
pub struct LmdbConfig {
    /// Maximum database size (in bytes)
    pub map_size: usize,
    /// Maximum number of readers
    pub max_readers: u32,
    /// Maximum number of named databases
    pub max_dbs: u32,
    /// LMDB environment flags
    pub flags: EnvFlags,
}

impl Default for LmdbConfig {
    fn default() -> Self {
        Self {
            // 32GB on 64-bit, ~4GB on 32-bit
            #[cfg(target_pointer_width = "64")]
            map_size: 32 * 1024 * 1024 * 1024,
            #[cfg(target_pointer_width = "32")]
            map_size: 0xFFFFF000,
            
            max_readers: 126, // LMDB default
            max_dbs: 20,     // Current requirement is 19
            flags: EnvFlags::empty(), // Safe default
        }
    }
}

impl LmdbConfig {
    /// Create a new configuration with default values
    pub fn new() -> Self {
        Self::default()
    }
    
    /// Create a configuration optimized for benchmarks
    /// 
    /// WARNING: This configuration trades durability for performance.
    /// Data may be lost on system crash.
    pub fn benchmark() -> Self {
        Self {
            flags: EnvFlags::NO_SYNC | EnvFlags::WRITE_MAP | EnvFlags::MAP_ASYNC,
            ..Self::default()
        }
    }
    
    /// Create a configuration for development/testing
    /// 
    /// WARNING: NO_SYNC flag means data is not flushed to disk.
    /// Data may be lost on system crash.
    pub fn development() -> Self {
        Self {
            flags: EnvFlags::NO_SYNC,
            ..Self::default()
        }
    }
    
    /// Create a high-performance production configuration
    /// 
    /// Uses WRITE_MAP and MAP_ASYNC for better performance
    /// while maintaining crash safety (application crashes won't lose data)
    pub fn high_performance() -> Self {
        Self {
            map_size: 10 * 1024 * 1024 * 1024 * 1024, // 10TB like strfry
            max_readers: 256,
            max_dbs: 64,
            flags: EnvFlags::WRITE_MAP | EnvFlags::MAP_ASYNC,
        }
    }
    
    /// Set the map size (virtual memory allocation)
    pub fn map_size(mut self, size: usize) -> Self {
        self.map_size = size;
        self
    }
    
    /// Set the maximum number of readers
    pub fn max_readers(mut self, readers: u32) -> Self {
        self.max_readers = readers;
        self
    }
    
    /// Set the maximum number of databases
    pub fn max_dbs(mut self, dbs: u32) -> Self {
        self.max_dbs = dbs;
        self
    }
    
    /// Set custom flags
    /// 
    /// WARNING: Some flags like NO_SYNC can cause data loss on system crash.
    pub fn flags(mut self, flags: EnvFlags) -> Self {
        self.flags = flags;
        self
    }
    
    /// Add flags to existing configuration
    pub fn add_flags(mut self, flags: EnvFlags) -> Self {
        self.flags |= flags;
        self
    }
    
    /// Check if NO_SYNC flag is set (data not flushed to disk)
    pub fn is_no_sync(&self) -> bool {
        self.flags.contains(EnvFlags::NO_SYNC)
    }
    
    /// Create configuration from environment variables
    /// 
    /// Supported variables:
    /// - NOSTR_LMDB_MAP_SIZE: Map size in GB (e.g., "32" for 32GB)
    /// - NOSTR_LMDB_MAX_READERS: Maximum readers (e.g., "256")
    /// - NOSTR_LMDB_MAX_DBS: Maximum databases (e.g., "64")
    /// - NOSTR_LMDB_MODE: "production", "development", "benchmark", "high_performance"
    pub fn from_env() -> Self {
        let mut config = Self::default();
        
        // Parse map size
        if let Ok(size_gb) = std::env::var("NOSTR_LMDB_MAP_SIZE") {
            if let Ok(gb) = size_gb.parse::<usize>() {
                config.map_size = gb * 1024 * 1024 * 1024;
            }
        }
        
        // Parse max readers
        if let Ok(readers) = std::env::var("NOSTR_LMDB_MAX_READERS") {
            if let Ok(r) = readers.parse::<u32>() {
                config.max_readers = r;
            }
        }
        
        // Parse max dbs
        if let Ok(dbs) = std::env::var("NOSTR_LMDB_MAX_DBS") {
            if let Ok(d) = dbs.parse::<u32>() {
                config.max_dbs = d;
            }
        }
        
        // Parse mode
        if let Ok(mode) = std::env::var("NOSTR_LMDB_MODE") {
            config = match mode.as_str() {
                "benchmark" => Self::benchmark(),
                "development" => Self::development(),
                "high_performance" => Self::high_performance(),
                _ => config, // Keep current config
            };
        }
        
        config
    }
}