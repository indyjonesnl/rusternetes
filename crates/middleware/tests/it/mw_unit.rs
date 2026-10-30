//! Unit tests moved out of `src/` so the lib is not compiled a second time as a test target.
#![allow(non_snake_case)]
mod cbor;
mod lib;
mod request_header;
mod response;
mod secure_serving;
mod secure_serving__authn;
mod secure_serving__authz;
mod secure_serving__filters;
mod secure_serving__options;
mod secure_serving__serve;
mod secure_serving__webhook;
mod table;
