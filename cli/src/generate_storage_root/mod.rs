#[cfg(test)]
mod smoke_test;

use clap::Args;
use eyre::Result;
use alloy_primitives::B256;
use alloy_rlp::Decodable;
use hex;
use rust_eth_triedb_pathdb::{PathDB, PathProviderConfig};
use rust_eth_triedb_common::TrieDatabase;
use rust_eth_triedb_state_trie::StateAccount;
use rust_eth_triedb_state_trie::encoding::{hex_to_keybytes, account_trie_node_key};
use rust_eth_triedb_state_trie::node::{Node, init_empty_root_node};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc};
use std::thread;

/// Generate storage root from a directory
#[derive(Debug, Clone, Args)]
#[command(name = "generate-storage-root")]
pub struct GenerateStorageRootArgs {
    /// Directory path containing the storage data
    #[arg(long, value_name = "DIR", help = "Directory path to triedb")]
    pub triedb_path: PathBuf,
}

impl GenerateStorageRootArgs {
    /// Execute the generate storage root command
    pub fn execute(&self) -> Result<()> {
        tracing::info!("Generating storage root from directory: {:?}", self.triedb_path);

        // Validate that the directory exists
        if !self.triedb_path.exists() {
            let err = eyre::eyre!("Directory does not exist: {:?}", self.triedb_path);
            tracing::error!("{}", err);
            return Err(err);
        }

        if !self.triedb_path.is_dir() {
            let err = eyre::eyre!("Path is not a directory: {:?}", self.triedb_path);
            tracing::error!("{}", err);
            return Err(err);
        }

        let path_str = self.triedb_path.to_str()
            .ok_or_else(|| eyre::eyre!("Invalid path: {:?}", self.triedb_path))?;

        let mut config = PathProviderConfig::default();
        config.trie_node_cache_size = 0;
        config.storage_root_cache_size = 0;

        let pathdb = PathDB::new(&path_str, config).unwrap();

        init_empty_root_node();
        let mut main_task = GenerateStorageRootMainTask::new("main_task".to_string(), pathdb);
        main_task.start()?;

        tracing::info!("Generated storage root finished!!!");
        Ok(())
    }
}

/// Main task that receives messages from sub-tasks
pub struct GenerateStorageRootMainTask {
    /// Name of the main task
    pub name: String,
    /// Trie of the main task
    pub root: B256,
    /// Count of sub-tasks
    pub sub_task_count: u64,
    /// Sub-tasks of the main task
    pub sub_tasks: HashMap<u64, GenerateStorageRootSubTask>,
    /// PathDB for the main task
    pub path_db: PathDB,
    /// Count of generated storage roots
    pub generate_storage_root_count: u64,
}

impl GenerateStorageRootMainTask {
    /// Create a new main task with a channel receiver
    pub fn new(name: String, pathdb: PathDB) -> Self {
        let (latest_block_number, latest_state_root) = pathdb.latest_persist_state().unwrap();
        tracing::info!("Latest persist state: block number: {}, state root: {}", latest_block_number, latest_state_root);
                
        let task = Self {
            name: name.clone(),
            root: latest_state_root,
            sub_task_count: 0,
            sub_tasks: HashMap::new(),
            path_db: pathdb,
            generate_storage_root_count: 0,
        };
        task
    }

    pub fn start(&mut self) -> Result<()> {
        tracing::info!("Starting main task: 0x{}", hex::encode(&self.name));
        let root_arc = self.resolve_hash_node(&self.root, &[])?;
        let root = Arc::try_unwrap(root_arc).unwrap_or_else(|arc| (*arc).clone());
        self.iterate_main_trie(root, &[], 0)?;

        tracing::info!("Sub tasks count: {}", self.sub_tasks.len());
        let mut sub_tasks_vec: Vec<_> = self.sub_tasks.drain().collect();
        sub_tasks_vec.sort_by_key(|(id, _)| *id);
        for (id, sub_task) in &sub_tasks_vec {
            tracing::info!("Sub task: id: {}, name: 0x{}", id, hex::encode(&sub_task.name));
        }

        let handles: Vec<_> = sub_tasks_vec.into_iter().map(|(id, mut sub_task)| {
            thread::spawn(move || {
                tracing::info!("Starting sub task: id: {}, name: 0x{}", id, hex::encode(&sub_task.name));
                let result = sub_task.start();
                if let Err(e) = &result {
                    panic!("Sub task {} failed, name: 0x{}: {}", id, hex::encode(&sub_task.name), e);
                } else {
                    tracing::info!("Sub task {} completed, name: 0x{}", id, hex::encode(&sub_task.name));
                }
                (id, result, sub_task)
            })
        }).collect();

        let mut completed_sub_tasks = HashMap::new();
        for handle in handles {
            let (id, result, sub_task) = handle.join().map_err(|e| eyre::eyre!("Thread panicked: {:?}", e))?;
            result?;
            tracing::info!("Sub task {} finished successfully", id);
            completed_sub_tasks.insert(id, sub_task);
        }

        // Update self.sub_tasks with completed tasks for statistics
        self.sub_tasks = completed_sub_tasks;
        self.print_progress()?;
        return Ok(());
    }

    pub fn iterate_main_trie(&mut self, node: Node, key: &[u8], depth: u64) -> Result<()> {
        // Helper to extract Node from Arc
        let unwrap_node = |arc_node: Arc<Node>| -> Node {
            Arc::try_unwrap(arc_node).unwrap_or_else(|arc| (*arc).clone())
        };
        
        match node {
            Node::Full(full_node) => {
                if depth < 2 {
                    for i in 0..17 {
                        let child_arc = full_node.children[i].clone();
                        let child = unwrap_node(child_arc);
                        let mut child_key = key.to_vec();
                        child_key.push(i as u8);
                        self.iterate_main_trie(child, &child_key, depth + 1)?;
                    }
                    drop(full_node);
                    return Ok(());
                } else {
                    panic!("main task depth >= 2 find full node, depth: {}", depth);
                }
            }
            Node::Short(short_node) => {
                if depth < 2 {
                    let mut hex_key = key.to_vec();
                    hex_key.extend(short_node.key.clone());
                    let val_arc = short_node.val.clone();
                    let val = unwrap_node(val_arc);
                    drop(short_node);
                    return self.iterate_main_trie(val, &hex_key, depth + 1);
                } else {
                    panic!("main task depth >= 2 find short node, depth: {}", depth);
                }
            }
            Node::Hash(hash_node) => {
                if depth < 2 {
                    let resolved_node_arc = self.resolve_hash_node(&hash_node, key)
                        .map_err(|e| eyre::eyre!("Failed to resolve hash node: {:?}, error: {}", key, e))?;
                    let resolved_node = unwrap_node(resolved_node_arc);
                    return self.iterate_main_trie(resolved_node, key, depth);
                } else if depth == 2 {
                    let sub_task = GenerateStorageRootSubTask::new(
                        self.sub_task_count, 
                        key.to_vec(),  
                        node.clone(), 
                        depth, 
                        self.path_db.clone());

                    let sub_task_id = sub_task.id;
                    self.sub_task_count += 1;
                    self.sub_tasks.insert(sub_task.id, sub_task);

                    tracing::info!("Create sub task under hash node, id: {}, key: 0x{}", sub_task_id, hex::encode(key));
                    return Ok(());
                } else {
                    panic!("main task depth > 2 find hash node, depth: {}", depth);
                }
            }
            Node::Value(value_node) => {
                let account = StateAccount::decode(&mut &value_node[..])
                    .map_err(|_| eyre::eyre!("Failed to decode account: {:?}", key))?;

                let hashed_address = hex_to_keybytes(key);
                self.path_db.put_storage_root(B256::from_slice(&hashed_address), account.storage_root)
                    .map_err(|e| eyre::eyre!("Failed to put storage root: {:?}, error: {}", key, e))?;

                self.generate_storage_root_count += 1;

                tracing::info!("Find value in main task, key: 0x{}, storage root: {}", hex::encode(key), account.storage_root);
                return Ok(());
            }
            Node::Empty => {
                tracing::info!("Find empty node in main task, key: 0x{}", hex::encode(key));
                return Ok(());
            }
        }
    }

    pub fn resolve_hash_node(&self, hash: &B256, prefix: &[u8]) -> Result<Arc<Node>> {
        let key = account_trie_node_key(prefix);
        let node_blob = self.path_db.get_trie_node(&key)
            .map_err(|e| eyre::eyre!("Failed to get trie node: prefix: 0x{}, error: {}", hex::encode(prefix), e))?;

        match node_blob {
            Some(node_blob) => {
                return Ok(Node::must_decode_node(Some(*hash), &node_blob));
            }
            None => {
                panic!("Failed to get trie node: {:?}", key);
            }
        }
    }

    pub fn print_progress(&mut self) -> Result<()> {
        let mut sub_tasks_vec: Vec<_> = self.sub_tasks.drain().collect();
        sub_tasks_vec.sort_by_key(|(id, _)| *id);

        let mut total_generate_storage_root_count = self.generate_storage_root_count;
        let mut total_max_depth = 0;
        for (_, sub_task) in &sub_tasks_vec {
            total_generate_storage_root_count += sub_task.generate_storage_root_count;
            if total_max_depth < sub_task.max_depth {
                total_max_depth = sub_task.max_depth;
            }
            sub_task.print_progress()?;
        }
        tracing::info!("Total generate storage root count: {}, total max depth: {}", total_generate_storage_root_count, total_max_depth);
        Ok(())
    }
}

/// Sub-task that sends messages to the main task
pub struct GenerateStorageRootSubTask {
    /// ID of the sub-task
    pub id: u64,
    /// Name of the sub-task
    pub name: Vec<u8>,
    /// Depth of the sub-task
    pub depth: u64,
    /// Max depth of the sub-task
    pub max_depth: u64,
    /// Sub-root of the sub-task
    pub sub_root: Node,
    /// PathDB for the sub-task
    pub path_db: PathDB,
    /// Count of generated storage roots
    pub generate_storage_root_count: u64,
}

impl GenerateStorageRootSubTask {
    /// Create a new sub-task with a channel sender
    pub fn new(
        id: u64, 
        name: Vec<u8>, 
        sub_root: Node,
        depth: u64,  
        path_db: PathDB) -> Self {
        // Extract Node from Arc to avoid Arc wrapping
        Self {
            id,
            name: name.clone(),
            depth,
            max_depth: 0,
            sub_root,
            path_db,
            generate_storage_root_count: 0,
        }
    }

    pub fn start(&mut self) -> Result<()> {
        let name = self.name.clone();
        self.iterate_sub_trie(self.sub_root.clone(), &name, self.depth)?;
        return Ok(());
    }

    pub fn iterate_sub_trie(&mut self, node: Node, key: &[u8], depth: u64) -> Result<()> {
        // Helper to extract Node from Arc
        let unwrap_node = |arc_node: Arc<Node>| -> Node {
            Arc::try_unwrap(arc_node).unwrap_or_else(|arc| (*arc).clone())
        };
        
        match node {
            Node::Full(full_node) => {
                for i in 0..17 {
                    let child_arc = full_node.children[i].clone();
                    let child = unwrap_node(child_arc);
                    let mut child_key = key.to_vec();
                    child_key.push(i as u8);
                    self.iterate_sub_trie(child, &child_key, depth + 1)?;
                }
                drop(full_node);
                return Ok(());
            }
            Node::Short(short_node) => {
                let mut hex_key = key.to_vec();
                hex_key.extend(short_node.key.clone());
                let val_arc = short_node.val.clone();
                let val = unwrap_node(val_arc);
                drop(short_node);
                return self.iterate_sub_trie(val, &hex_key, depth + 1);
            }
            Node::Hash(hash_node) => {
                let resolved_node_arc = self.resolve_hash_node(&hash_node, key)
                    .map_err(|e| eyre::eyre!("Failed to resolve hash node: {:?}, error: {}", key, e))?;
                let resolved_node = unwrap_node(resolved_node_arc);
                return self.iterate_sub_trie(resolved_node, key, depth);
            }
            Node::Value(value_node) => {
                let account = StateAccount::decode(&mut &value_node[..])
                    .map_err(|_| eyre::eyre!("Failed to decode account: {:?}", key))?;

                drop(value_node);

                let hashed_address = hex_to_keybytes(key);
                self.path_db.put_storage_root(B256::from_slice(&hashed_address), account.storage_root)
                    .map_err(|e| eyre::eyre!("Failed to put storage root: {:?}, error: {}", key, e))?;

                self.generate_storage_root_count += 1;
                if self.max_depth < depth {
                    self.max_depth = depth;
                }

                if self.generate_storage_root_count % 10000 == 0 {
                    self.print_progress()?;
                }
                return Ok(());
            }
            Node::Empty => {
                return Ok(());
            }
        }
    }

    pub fn resolve_hash_node(&self, hash: &B256, prefix: &[u8]) -> Result<Arc<Node>> {
        let key = account_trie_node_key(prefix);
        let node_blob = self.path_db.get_trie_node(&key)
            .map_err(|e| eyre::eyre!("Failed to get trie node: prefix: 0x{}, error: {}", hex::encode(prefix), e))?;

        match node_blob {
            Some(node_blob) => {
                return Ok(Node::must_decode_node(Some(*hash), &node_blob));
            }
            None => {
                panic!("Failed to get trie node: {:?}", key);
            }
        }
    }

    pub fn print_progress(&self) -> Result<()> {
        tracing::info!("Sub task: id: {}, name: 0x{}, max depth: {}, generate storage root count: {}", self.id, hex::encode(&self.name), self.max_depth, self.generate_storage_root_count);
        return Ok(());
    }
}


