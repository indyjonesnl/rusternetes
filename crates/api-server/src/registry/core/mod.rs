//! Strategies and stores for the core (`""`) API group — upstream's
//! `pkg/registry/core/<resource>/`.

pub mod configmap;
pub mod endpoint;
pub mod limitrange;
pub mod namespace;
pub mod node;
pub mod persistentvolume;
pub mod persistentvolumeclaim;
pub mod podtemplate;
pub mod replicationcontroller;
pub mod resourcequota;
pub mod secret;
pub mod service;
pub mod serviceaccount;
