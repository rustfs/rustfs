use std::{collections::HashMap, io::Cursor, time::SystemTime};

use crate::{
    admin_server_info::ServerProperties, endpoints::EndpointServerPools, global::is_dist_erasure,
    heal::heal_commands::BgHealState, store_api::StorageInfo,
};
use common::error::{Error, Result};
use madmin::{
    health::{Cpus, MemInfo, OsInfo, Partitions, ProcInfo, SysConfig, SysErrors, SysService},
    metrics::RealtimeMetrics,
    net::NetInfo,
};
use protos::{
    node_service_time_out_client,
    proto_gen::node_service::{
        BackgroundHealStatusRequest, DeleteBucketMetadataRequest, DeletePolicyRequest, DeleteServiceAccountRequest,
        DeleteUserRequest, GetCpusRequest, GetMemInfoRequest, GetMetricsRequest, GetNetInfoRequest, GetOsInfoRequest,
        GetPartitionsRequest, GetProcInfoRequest, GetSeLinuxInfoRequest, GetSysConfigRequest, GetSysErrorsRequest,
        LoadBucketMetadataRequest, LoadGroupRequest, LoadPolicyMappingRequest, LoadPolicyRequest, LoadRebalanceMetaRequest,
        LoadServiceAccountRequest, LoadTransitionTierConfigRequest, LoadUserRequest, LocalStorageInfoRequest, Mss,
        ReloadPoolMetaRequest, ReloadSiteReplicationConfigRequest, ServerInfoRequest, SignalServiceRequest,
        StartProfilingRequest, StopRebalanceRequest,
    },
};
use rmp_serde::{Deserializer, Serializer};
use serde::{Deserialize, Serialize as _};
use tonic::Request;

pub const PEER_RESTSIGNAL: &str = "signal";
pub const PEER_RESTSUB_SYS: &str = "sub-sys";
pub const PEER_RESTDRY_RUN: &str = "dry-run";

pub struct PeerRestClient {
    addr: String,
}

impl PeerRestClient {
    pub fn new(url: url::Url) -> Self {
        Self {
            addr: format!("{}://{}:{}", url.scheme(), url.host_str().unwrap(), url.port().unwrap()),
        }
    }
    pub async fn new_clients(_eps: EndpointServerPools) -> (Vec<Self>, Vec<Self>) {
        if !is_dist_erasure().await {
            return (Vec::new(), Vec::new());
        }

        // FIXME:TODO

        todo!()
    }
}

impl PeerRestClient {
    pub async fn local_storage_info(&self) -> Result<StorageInfo> {
        let mut client = node_service_time_out_client(&self.addr)
            .await
            .map_err(|err| Error::msg(err.to_string()))?;
        let request = Request::new(LocalStorageInfoRequest { metrics: true });

        let response = client.local_storage_info(request).await?.into_inner();
        if !response.success {
            if let Some(msg) = response.error_info {
                return Err(Error::msg(msg));
            }
            return Err(Error::msg(""));
        }
        let data = response.storage_info;

        let mut buf = Deserializer::new(Cursor::new(data));
        let storage_info: StorageInfo = Deserialize::deserialize(&mut buf).unwrap();

        Ok(storage_info)
    }

    pub async fn server_info(&self) -> Result<ServerProperties> {
        let mut client = node_service_time_out_client(&self.addr)
            .await
            .map_err(|err| Error::msg(err.to_string()))?;
        let request = Request::new(ServerInfoRequest { metrics: true });

        let response = client.server_info(request).await?.into_inner();
        if !response.success {
            if let Some(msg) = response.error_info {
                return Err(Error::msg(msg));
            }
            return Err(Error::msg(""));
        }
        let data = response.server_properties;

        let mut buf = Deserializer::new(Cursor::new(data));
        let storage_properties: ServerProperties = Deserialize::deserialize(&mut buf).unwrap();

        Ok(storage_properties)
    }

    pub async fn get_cpus(&self) -> Result<Cpus> {
        let mut client = node_service_time_out_client(&self.addr)
            .await
            .map_err(|err| Error::msg(err.to_string()))?;
        let request = Request::new(GetCpusRequest {});

        let response = client.get_cpus(request).await?.into_inner();
        if !response.success {
            if let Some(msg) = response.error_info {
                return Err(Error::msg(msg));
            }
            return Err(Error::msg(""));
        }
        let data = response.cpus;

        let mut buf = Deserializer::new(Cursor::new(data));
        let cpus: Cpus = Deserialize::deserialize(&mut buf).unwrap();

        Ok(cpus)
    }

    pub async fn get_net_info(&self) -> Result<NetInfo> {
        let mut client = node_service_time_out_client(&self.addr)
            .await
            .map_err(|err| Error::msg(err.to_string()))?;
        let request = Request::new(GetNetInfoRequest {});

        let response = client.get_net_info(request).await?.into_inner();
        if !response.success {
            if let Some(msg) = response.error_info {
                return Err(Error::msg(msg));
            }
            return Err(Error::msg(""));
        }
        let data = response.net_info;

        let mut buf = Deserializer::new(Cursor::new(data));
        let net_info: NetInfo = Deserialize::deserialize(&mut buf).unwrap();

        Ok(net_info)
    }

    pub async fn get_partitions(&self) -> Result<Partitions> {
        let mut client = node_service_time_out_client(&self.addr)
            .await
            .map_err(|err| Error::msg(err.to_string()))?;
        let request = Request::new(GetPartitionsRequest {});

        let response = client.get_partitions(request).await?.into_inner();
        if !response.success {
            if let Some(msg) = response.error_info {
                return Err(Error::msg(msg));
            }
            return Err(Error::msg(""));
        }
        let data = response.partitions;

        let mut buf = Deserializer::new(Cursor::new(data));
        let partitions: Partitions = Deserialize::deserialize(&mut buf).unwrap();

        Ok(partitions)
    }

    pub async fn get_os_info(&self) -> Result<OsInfo> {
        let mut client = node_service_time_out_client(&self.addr)
            .await
            .map_err(|err| Error::msg(err.to_string()))?;
        let request = Request::new(GetOsInfoRequest {});

        let response = client.get_os_info(request).await?.into_inner();
        if !response.success {
            if let Some(msg) = response.error_info {
                return Err(Error::msg(msg));
            }
            return Err(Error::msg(""));
        }
        let data = response.os_info;

        let mut buf = Deserializer::new(Cursor::new(data));
        let os_info: OsInfo = Deserialize::deserialize(&mut buf).unwrap();

        Ok(os_info)
    }

    pub async fn get_se_linux_info(&self) -> Result<SysService> {
        let mut client = node_service_time_out_client(&self.addr)
            .await
            .map_err(|err| Error::msg(err.to_string()))?;
        let request = Request::new(GetSeLinuxInfoRequest {});

        let response = client.get_se_linux_info(request).await?.into_inner();
        if !response.success {
            if let Some(msg) = response.error_info {
                return Err(Error::msg(msg));
            }
            return Err(Error::msg(""));
        }
        let data = response.sys_services;

        let mut buf = Deserializer::new(Cursor::new(data));
        let sys_services: SysService = Deserialize::deserialize(&mut buf).unwrap();

        Ok(sys_services)
    }

    pub async fn get_sys_config(&self) -> Result<SysConfig> {
        let mut client = node_service_time_out_client(&self.addr)
            .await
            .map_err(|err| Error::msg(err.to_string()))?;
        let request = Request::new(GetSysConfigRequest {});

        let response = client.get_sys_config(request).await?.into_inner();
        if !response.success {
            if let Some(msg) = response.error_info {
                return Err(Error::msg(msg));
            }
            return Err(Error::msg(""));
        }
        let data = response.sys_config;

        let mut buf = Deserializer::new(Cursor::new(data));
        let sys_config: SysConfig = Deserialize::deserialize(&mut buf).unwrap();

        Ok(sys_config)
    }

    pub async fn get_sys_errors(&self) -> Result<SysErrors> {
        let mut client = node_service_time_out_client(&self.addr)
            .await
            .map_err(|err| Error::msg(err.to_string()))?;
        let request = Request::new(GetSysErrorsRequest {});

        let response = client.get_sys_errors(request).await?.into_inner();
        if !response.success {
            if let Some(msg) = response.error_info {
                return Err(Error::msg(msg));
            }
            return Err(Error::msg(""));
        }
        let data = response.sys_errors;

        let mut buf = Deserializer::new(Cursor::new(data));
        let sys_errors: SysErrors = Deserialize::deserialize(&mut buf).unwrap();

        Ok(sys_errors)
    }

    pub async fn get_mem_info(&self) -> Result<MemInfo> {
        let mut client = node_service_time_out_client(&self.addr)
            .await
            .map_err(|err| Error::msg(err.to_string()))?;
        let request = Request::new(GetMemInfoRequest {});

        let response = client.get_mem_info(request).await?.into_inner();
        if !response.success {
            if let Some(msg) = response.error_info {
                return Err(Error::msg(msg));
            }
            return Err(Error::msg(""));
        }
        let data = response.mem_info;

        let mut buf = Deserializer::new(Cursor::new(data));
        let mem_info: MemInfo = Deserialize::deserialize(&mut buf).unwrap();

        Ok(mem_info)
    }

    pub async fn get_metrics(&self, t: MetricType, opts: &CollectMetricsOpts) -> Result<RealtimeMetrics> {
        let mut client = node_service_time_out_client(&self.addr)
            .await
            .map_err(|err| Error::msg(err.to_string()))?;
        let mut buf_t = Vec::new();
        t.serialize(&mut Serializer::new(&mut buf_t))?;
        let mut buf_o = Vec::new();
        opts.serialize(&mut Serializer::new(&mut buf_o))?;
        let request = Request::new(GetMetricsRequest {
            metric_type: buf_t,
            opts: buf_o,
        });

        let response = client.get_metrics(request).await?.into_inner();
        if !response.success {
            if let Some(msg) = response.error_info {
                return Err(Error::msg(msg));
            }
            return Err(Error::msg(""));
        }
        let data = response.realtime_metrics;

        let mut buf = Deserializer::new(Cursor::new(data));
        let realtime_metrics: RealtimeMetrics = Deserialize::deserialize(&mut buf).unwrap();

        Ok(realtime_metrics)
    }

    pub async fn get_proc_info(&self) -> Result<ProcInfo> {
        let mut client = node_service_time_out_client(&self.addr)
            .await
            .map_err(|err| Error::msg(err.to_string()))?;
        let request = Request::new(GetProcInfoRequest {});

        let response = client.get_proc_info(request).await?.into_inner();
        if !response.success {
            if let Some(msg) = response.error_info {
                return Err(Error::msg(msg));
            }
            return Err(Error::msg(""));
        }
        let data = response.proc_info;

        let mut buf = Deserializer::new(Cursor::new(data));
        let proc_info: ProcInfo = Deserialize::deserialize(&mut buf).unwrap();

        Ok(proc_info)
    }

    pub async fn start_profiling(&self, profiler: &str) -> Result<()> {
        let mut client = node_service_time_out_client(&self.addr)
            .await
            .map_err(|err| Error::msg(err.to_string()))?;
        let request = Request::new(StartProfilingRequest {
            profiler: profiler.to_string(),
        });

        let response = client.start_profiling(request).await?.into_inner();
        if !response.success {
            if let Some(msg) = response.error_info {
                return Err(Error::msg(msg));
            }
            return Err(Error::msg(""));
        }
        Ok(())
    }

    pub async fn download_profile_data(&self) -> Result<()> {
        todo!()
    }

    pub async fn get_bucket_stats(&self) -> Result<()> {
        todo!()
    }

    pub async fn get_sr_metrics(&self) -> Result<()> {
        todo!()
    }

    pub async fn get_all_bucket_stats(&self) -> Result<()> {
        todo!()
    }

    pub async fn load_bucket_metadata(&self, bucket: &str) -> Result<()> {
        let mut client = node_service_time_out_client(&self.addr)
            .await
            .map_err(|err| Error::msg(err.to_string()))?;
        let request = Request::new(LoadBucketMetadataRequest {
            bucket: bucket.to_string(),
        });

        let response = client.load_bucket_metadata(request).await?.into_inner();
        if !response.success {
            if let Some(msg) = response.error_info {
                return Err(Error::msg(msg));
            }
            return Err(Error::msg(""));
        }
        Ok(())
    }

    pub async fn delete_bucket_metadata(&self, bucket: &str) -> Result<()> {
        let mut client = node_service_time_out_client(&self.addr)
            .await
            .map_err(|err| Error::msg(err.to_string()))?;
        let request = Request::new(DeleteBucketMetadataRequest {
            bucket: bucket.to_string(),
        });

        let response = client.delete_bucket_metadata(request).await?.into_inner();
        if !response.success {
            if let Some(msg) = response.error_info {
                return Err(Error::msg(msg));
            }
            return Err(Error::msg(""));
        }
        Ok(())
    }

    pub async fn delete_policy(&self, policy: &str) -> Result<()> {
        let mut client = node_service_time_out_client(&self.addr)
            .await
            .map_err(|err| Error::msg(err.to_string()))?;
        let request = Request::new(DeletePolicyRequest {
            policy_name: policy.to_string(),
        });

        let response = client.delete_policy(request).await?.into_inner();
        if !response.success {
            if let Some(msg) = response.error_info {
                return Err(Error::msg(msg));
            }
            return Err(Error::msg(""));
        }
        Ok(())
    }

    pub async fn load_policy(&self, policy: &str) -> Result<()> {
        let mut client = node_service_time_out_client(&self.addr)
            .await
            .map_err(|err| Error::msg(err.to_string()))?;
        let request = Request::new(LoadPolicyRequest {
            policy_name: policy.to_string(),
        });

        let response = client.load_policy(request).await?.into_inner();
        if !response.success {
            if let Some(msg) = response.error_info {
                return Err(Error::msg(msg));
            }
            return Err(Error::msg(""));
        }
        Ok(())
    }

    pub async fn load_policy_mapping(&self, user_or_group: &str, user_type: u64, is_group: bool) -> Result<()> {
        let mut client = node_service_time_out_client(&self.addr)
            .await
            .map_err(|err| Error::msg(err.to_string()))?;
        let request = Request::new(LoadPolicyMappingRequest {
            user_or_group: user_or_group.to_string(),
            user_type,
            is_group,
        });

        let response = client.load_policy_mapping(request).await?.into_inner();
        if !response.success {
            if let Some(msg) = response.error_info {
                return Err(Error::msg(msg));
            }
            return Err(Error::msg(""));
        }
        Ok(())
    }

    pub async fn delete_user(&self, access_key: &str) -> Result<()> {
        let mut client = node_service_time_out_client(&self.addr)
            .await
            .map_err(|err| Error::msg(err.to_string()))?;
        let request = Request::new(DeleteUserRequest {
            access_key: access_key.to_string(),
        });

        let response = client.delete_user(request).await?.into_inner();
        if !response.success {
            if let Some(msg) = response.error_info {
                return Err(Error::msg(msg));
            }
            return Err(Error::msg(""));
        }
        Ok(())
    }

    pub async fn delete_service_account(&self, access_key: &str) -> Result<()> {
        let mut client = node_service_time_out_client(&self.addr)
            .await
            .map_err(|err| Error::msg(err.to_string()))?;
        let request = Request::new(DeleteServiceAccountRequest {
            access_key: access_key.to_string(),
        });

        let response = client.delete_service_account(request).await?.into_inner();
        if !response.success {
            if let Some(msg) = response.error_info {
                return Err(Error::msg(msg));
            }
            return Err(Error::msg(""));
        }
        Ok(())
    }

    pub async fn load_user(&self, access_key: &str, temp: bool) -> Result<()> {
        let mut client = node_service_time_out_client(&self.addr)
            .await
            .map_err(|err| Error::msg(err.to_string()))?;
        let request = Request::new(LoadUserRequest {
            access_key: access_key.to_string(),
            temp,
        });

        let response = client.load_user(request).await?.into_inner();
        if !response.success {
            if let Some(msg) = response.error_info {
                return Err(Error::msg(msg));
            }
            return Err(Error::msg(""));
        }
        Ok(())
    }

    pub async fn load_service_account(&self, access_key: &str) -> Result<()> {
        let mut client = node_service_time_out_client(&self.addr)
            .await
            .map_err(|err| Error::msg(err.to_string()))?;
        let request = Request::new(LoadServiceAccountRequest {
            access_key: access_key.to_string(),
        });

        let response = client.load_service_account(request).await?.into_inner();
        if !response.success {
            if let Some(msg) = response.error_info {
                return Err(Error::msg(msg));
            }
            return Err(Error::msg(""));
        }
        Ok(())
    }

    pub async fn load_group(&self, group: &str) -> Result<()> {
        let mut client = node_service_time_out_client(&self.addr)
            .await
            .map_err(|err| Error::msg(err.to_string()))?;
        let request = Request::new(LoadGroupRequest {
            group: group.to_string(),
        });

        let response = client.load_group(request).await?.into_inner();
        if !response.success {
            if let Some(msg) = response.error_info {
                return Err(Error::msg(msg));
            }
            return Err(Error::msg(""));
        }
        Ok(())
    }

    pub async fn reload_site_replication_config(&self) -> Result<()> {
        let mut client = node_service_time_out_client(&self.addr)
            .await
            .map_err(|err| Error::msg(err.to_string()))?;
        let request = Request::new(ReloadSiteReplicationConfigRequest {});

        let response = client.reload_site_replication_config(request).await?.into_inner();
        if !response.success {
            if let Some(msg) = response.error_info {
                return Err(Error::msg(msg));
            }
            return Err(Error::msg(""));
        }
        Ok(())
    }

    pub async fn signal_service(&self, sig: u64, sub_sys: &str, dry_run: bool, _exec_at: SystemTime) -> Result<()> {
        let mut client = node_service_time_out_client(&self.addr)
            .await
            .map_err(|err| Error::msg(err.to_string()))?;
        let mut vars = HashMap::new();
        vars.insert(PEER_RESTSIGNAL.to_string(), sig.to_string());
        vars.insert(PEER_RESTSUB_SYS.to_string(), sub_sys.to_string());
        vars.insert(PEER_RESTDRY_RUN.to_string(), dry_run.to_string());
        let request = Request::new(SignalServiceRequest {
            vars: Some(Mss { value: vars }),
        });

        let response = client.signal_service(request).await?.into_inner();
        if !response.success {
            if let Some(msg) = response.error_info {
                return Err(Error::msg(msg));
            }
            return Err(Error::msg(""));
        }
        Ok(())
    }

    pub async fn background_heal_status(&self) -> Result<BgHealState> {
        let mut client = node_service_time_out_client(&self.addr)
            .await
            .map_err(|err| Error::msg(err.to_string()))?;
        let request = Request::new(BackgroundHealStatusRequest {});

        let response = client.background_heal_status(request).await?.into_inner();
        if !response.success {
            if let Some(msg) = response.error_info {
                return Err(Error::msg(msg));
            }
            return Err(Error::msg(""));
        }
        let data = response.bg_heal_state;

        let mut buf = Deserializer::new(Cursor::new(data));
        let bg_heal_state: BgHealState = Deserialize::deserialize(&mut buf).unwrap();

        Ok(bg_heal_state)
    }

    pub async fn get_metacache_listing(&self) -> Result<()> {
        let mut _client = node_service_time_out_client(&self.addr)
            .await
            .map_err(|err| Error::msg(err.to_string()))?;
        todo!()
    }

    pub async fn update_metacache_listing(&self) -> Result<()> {
        let mut _client = node_service_time_out_client(&self.addr)
            .await
            .map_err(|err| Error::msg(err.to_string()))?;
        todo!()
    }

    pub async fn reload_pool_meta(&self) -> Result<()> {
        let mut client = node_service_time_out_client(&self.addr)
            .await
            .map_err(|err| Error::msg(err.to_string()))?;
        let request = Request::new(ReloadPoolMetaRequest {});

        let response = client.reload_pool_meta(request).await?.into_inner();
        if !response.success {
            if let Some(msg) = response.error_info {
                return Err(Error::msg(msg));
            }
            return Err(Error::msg(""));
        }

        Ok(())
    }

    pub async fn stop_rebalance(&self) -> Result<()> {
        let mut client = node_service_time_out_client(&self.addr)
            .await
            .map_err(|err| Error::msg(err.to_string()))?;
        let request = Request::new(StopRebalanceRequest {});

        let response = client.stop_rebalance(request).await?.into_inner();
        if !response.success {
            if let Some(msg) = response.error_info {
                return Err(Error::msg(msg));
            }
            return Err(Error::msg(""));
        }

        Ok(())
    }

    pub async fn load_rebalance_meta(&self, start_rebalance: bool) -> Result<()> {
        let mut client = node_service_time_out_client(&self.addr)
            .await
            .map_err(|err| Error::msg(err.to_string()))?;
        let request = Request::new(LoadRebalanceMetaRequest { start_rebalance });

        let response = client.load_rebalance_meta(request).await?.into_inner();
        if !response.success {
            if let Some(msg) = response.error_info {
                return Err(Error::msg(msg));
            }
            return Err(Error::msg(""));
        }

        Ok(())
    }

    pub async fn load_transition_tier_config(&self) -> Result<()> {
        let mut client = node_service_time_out_client(&self.addr)
            .await
            .map_err(|err| Error::msg(err.to_string()))?;
        let request = Request::new(LoadTransitionTierConfigRequest {});

        let response = client.load_transition_tier_config(request).await?.into_inner();
        if !response.success {
            if let Some(msg) = response.error_info {
                return Err(Error::msg(msg));
            }
            return Err(Error::msg(""));
        }

        Ok(())
    }
}
