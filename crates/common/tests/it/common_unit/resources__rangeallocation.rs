//! Unit tests moved from `src/resources/rangeallocation.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::types::{ObjectMeta, TypeMeta};
    pub use serde::{Deserialize, Serialize};
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::resources::rangeallocation::*;

    #[test]
    fn data_round_trips_as_base64() {
        let r = RangeAllocation {
            range: "30000-32767".to_string(),
            data: vec![0x01, 0x80],
            ..Default::default()
        };
        let json = serde_json::to_value(&r).unwrap();
        assert_eq!(json["data"], "AYA=");
        assert_eq!(json["range"], "30000-32767");
        let back: RangeAllocation = serde_json::from_value(json).unwrap();
        assert_eq!(back, r);
        let nil: RangeAllocation =
            serde_json::from_value(serde_json::json!({"range": "", "data": null})).unwrap();
        assert!(nil.data.is_empty());
    }
}
