//! Unit tests moved out of `src/` so the lib is not compiled a second time as a test target.
#![allow(non_snake_case)]
mod cri_runtime;
mod cri_runtime__host_policy;
mod cri_runtime__image_volume_metrics;
mod cri_runtime__probe;
mod cri_runtime__runtime;
mod cri_runtime__status;
mod cri_runtime__translate;
mod fsquota;
mod fsquota__common;
mod node_ip__net_interface;
mod podcertificate__wiring;
mod volume_manager__cache__actual_state_of_world;
mod volume_manager__cache__desired_state_of_world;
mod volume_manager__cache__desired_state_of_world_selinux_metrics;
mod volume_manager__populator;
