//! Unit tests moved out of `src/` so the lib is not compiled a second time as a test target.
#![allow(non_snake_case)]
mod pluginmanager;
mod pluginmanager__cache;
mod pluginmanager__csi_handler;
mod pluginmanager__metrics;
mod pluginmanager__operation;
mod pluginmanager__watcher;
