use crate::omni_paxos::OmniPaxosConfig;
use crate::sequence_paxos::SequencePaxos;
use crate::storage::{Entry, Storage};
use crate::util::NodeId;
use crate::ProposeErr;

#[cfg(feature = "logging")]
use crate::utils::logger::create_logger;
#[cfg(feature = "logging")]
use slog::Logger;

// TODO: remove when NezhaProxy is fully used
#[allow(dead_code)]
pub(crate) struct NezhaProxy {
    pid: NodeId,
    peers: Vec<NodeId>,
    #[cfg(feature = "logging")]
    logger: Logger,
}

impl NezhaProxy {
    pub(crate) fn with(config: NezhaProxyConfig) -> Self {
        let pid = config.pid;
        let peers = config.peers;
        NezhaProxy {
            pid,
            peers,
            #[cfg(feature = "logging")]
            logger: {
                if let Some(logger) = config.custom_logger {
                    logger
                } else {
                    let s = config
                        .logger_file_path
                        .unwrap_or_else(|| format!("logs/paxos_{}.log", pid));
                    create_logger(s.as_str())
                }
            },
        }
    }

    /// TODO: a unique message id will need to be assigned and propagated further
    ///       the same identifier will need to be attached to fast/slow replies
    ///       so they can be mapped to the message which they correspond to
    pub(crate) fn append<T, B>(
        &self,
        seq_paxos: &mut SequencePaxos<T, B>,
        entry: T,
    ) -> Result<(), ProposeErr<T>>
    where
        T: Entry,
        B: Storage<T>,
    {
        seq_paxos.append(entry)
    }

    pub(crate) fn get_decided_idx<T, B>(&self, seq_paxos: &SequencePaxos<T, B>) -> usize
    where
        T: Entry,
        B: Storage<T>,
    {
        seq_paxos.get_decided_idx()
    }
}

/// Configuration for `NezhaProxy`
/// # Fields
/// * `pid`: The unique identifier of this node. Must not be 0.
/// * `peers`: The peers of this node i.e. the `pid`s of the other servers in the configuration.
/// * `logger_file_path`: The path where the default logger logs events.
/// * `custom_logger`: An optional user-provided logger implementation
#[derive(Clone, Debug)]
pub(crate) struct NezhaProxyConfig {
    pid: NodeId,
    peers: Vec<NodeId>,
    #[cfg(feature = "logging")]
    logger_file_path: Option<String>,
    #[cfg(feature = "logging")]
    custom_logger: Option<Logger>,
}

impl From<OmniPaxosConfig> for NezhaProxyConfig {
    fn from(config: OmniPaxosConfig) -> Self {
        let pid = config.server_config.pid;
        let peers = config
            .cluster_config
            .nodes
            .into_iter()
            .filter(|x| *x != pid)
            .collect();
        NezhaProxyConfig {
            pid,
            peers,
            #[cfg(feature = "logging")]
            logger_file_path: config.server_config.logger_file_path,
            #[cfg(feature = "logging")]
            custom_logger: config.server_config.custom_logger,
        }
    }
}
