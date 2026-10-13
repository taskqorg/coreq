//! Product connection wire v1: client checks shared by every consumer (docs/design/enrollment.md § Wire v1).

use serde_json::Value;

pub const CONTRACT: &str = "coreq-enrollment/1";
pub const PROTOCOL: u64 = 1;

/// A response of this major; any other or missing major is refused, unknown fields are ignored.
pub fn check(resp: &Value) -> Result<(), String> {
    match resp["protocol"].as_u64() {
        Some(PROTOCOL) => Ok(()),
        p => Err(format!("unsupported protocol {}", p.map_or("none".into(), |p| p.to_string()))),
    }
}

/// `Connected` only when the receipt is projected and names this installation, generation, schema and event.
pub fn verify(receipt: &Value, installation: &str, generation: u64, schema: &str, event_id: &str) -> Result<(), String> {
    check(receipt)?;
    for (k, want) in [("installation", installation), ("schema", schema), ("event_id", event_id)] {
        if receipt[k].as_str() != Some(want) {
            return Err(format!("{k} mismatch"));
        }
    }
    if receipt["generation"].as_u64() != Some(generation) {
        return Err("generation mismatch".into());
    }
    match receipt["state"].as_str() {
        Some("projected") => Ok(()),
        s => Err(format!("not projected: {}", s.unwrap_or("none"))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn check_refuses_other_major() {
        assert!(check(&json!({"protocol": 1, "future": true})).is_ok());
        assert_eq!(check(&json!({"protocol": 2})), Err("unsupported protocol 2".into()));
        assert_eq!(check(&json!({})), Err("unsupported protocol none".into()));
    }
}
