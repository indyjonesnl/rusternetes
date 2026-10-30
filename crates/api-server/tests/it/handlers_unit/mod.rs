//! Unit tests moved out of `src/` so the lib is not compiled a second time as a test target.
#![allow(non_snake_case)]
mod endpoints__handlers;
mod handlers;
mod handlers__aggregator;
mod handlers__authorization;
mod handlers__cel_validation;
mod handlers__configmap;
mod handlers__crd;
mod handlers__custom_metrics;
mod handlers__custom_resource;
mod handlers__defaults;
mod handlers__event;
mod handlers__filtering;
mod handlers__health;
mod handlers__list_options;
mod handlers__metrics;
mod handlers__node_conn;
mod handlers__node_restriction;
mod handlers__openapi;
mod handlers__pod_subresources;
mod handlers__poddisruptionbudget;
mod handlers__proxy;
mod handlers__ratcheting;
mod handlers__validation;
mod handlers__volumeattributesclass;
mod handlers__watch;
