//! Bounded, lossless partitioning of logical fixed-code counter summaries.
use super::{Event,Res,EVENT_BYTES,hex};
use serde_json::{Map,Value,json};

pub(super) const FIELDS:&[&str]=&["group_id","group_hash","part_index","part_count"];
const PARTS:usize=16;
const BYTES:usize=128<<10;
fn canonical(value: &mut Value) -> String {value.sort_all_objects();value.to_string()}

pub(super) fn part_id(group: &str,index: usize) -> String {if index==0 {group.to_owned()} else {crate::sha256_hex(format!("{group}\0count-part\0{index}").as_bytes())[..32].into()}}
pub(super) fn removal_ids(group: &str) -> Vec<String> {(0..PARTS).map(|index|part_id(group,index)).collect()}
pub(super) fn validate(value: &Value) -> Res<()> {
    if !FIELDS.iter().any(|key|value.get(*key).is_some()) {return Ok(());}
    if !value["group_id"].as_str().is_some_and(|s|hex(s,32))
        ||!value["group_hash"].as_str().is_some_and(|s|hex(s,64))
        ||!value["part_count"].as_u64().is_some_and(|n|(2..=PARTS as u64).contains(&n))
        ||!value["part_index"].as_u64().is_some_and(|n|value["part_count"].as_u64().is_some_and(|total|n<total))
        ||!value["counts"].as_object().is_some_and(|counts|!counts.is_empty()) {
        return Err("telemetry invalid count summary parts".into());
    }
    Ok(())
}

pub(super) fn split(mut value: Value,id: &str) -> Res<Vec<Event>> {
    super::validate(&value,None)?;
    if FIELDS.iter().any(|key|value.get(*key).is_some()) {return Err("telemetry invalid already-partitioned summary".into());}
    let payload=value.to_string();
    if payload.len()<=EVENT_BYTES {return Ok(vec![Event::with_id(value,Some(id))?]);}
    if payload.len()>BYTES {return Err("telemetry invalid logical summary size".into());}
    let payload=canonical(&mut value);
    let counts=value.as_object_mut().unwrap().remove("counts").ok_or("telemetry invalid oversized non-count observation")?;
    let counts=counts.as_object().ok_or("telemetry invalid count summary")?;
    value["counts"]=json!({});value["group_id"]=json!(id);value["group_hash"]=json!(crate::sha256_hex(payload.as_bytes()));
    value["part_index"]=json!(PARTS-1);value["part_count"]=json!(PARTS);
    let room=EVENT_BYTES.checked_sub(value.to_string().len()).ok_or("telemetry invalid summary metadata size")?;
    let mut chunks=Vec::new();let mut chunk=Map::new();let mut used=0;
    for (key,count) in counts {
        let bytes=serde_json::to_string(key).map_err(super::err)?.len()+1+count.to_string().len();
        if bytes>room {return Err("telemetry invalid summary counter size".into());}
        if !chunk.is_empty()&&used+1+bytes>room {chunks.push(std::mem::take(&mut chunk));used=0;}
        used+=bytes+usize::from(!chunk.is_empty());chunk.insert(key.clone(),count.clone());
        if chunks.len()>=PARTS {return Err("telemetry invalid summary part count".into());}
    }
    if !chunk.is_empty() {chunks.push(chunk);}
    if chunks.len()<2||chunks.len()>PARTS {return Err("telemetry invalid summary part count".into());}
    let total=chunks.len();let mut events=Vec::with_capacity(total);
    for (index,counts) in chunks.into_iter().enumerate() {
        let mut part=value.clone();part["counts"]=Value::Object(counts);part["part_index"]=json!(index);part["part_count"]=json!(total);
        let part_id=part_id(id,index);
        events.push(Event::with_id(part,Some(&part_id))?);
    }
    Ok(events)
}

/// Restore one logical summary. An incomplete retained group is unknown, never a partial total.
pub fn assemble(parts: &[Value]) -> Res<Value> {
    if parts.is_empty()||parts.len()>PARTS {return Err("telemetry invalid summary assembly size".into());}
    for part in parts {super::validate(part,None)?;if part.to_string().len()>EVENT_BYTES {return Err("telemetry event too large".into());}}
    if parts.len()==1&&parts[0].get("group_id").is_none() {return Ok(parts[0].clone());}
    let first=&parts[0];let total=first["part_count"].as_u64().ok_or("telemetry invalid summary group")? as usize;
    if parts.len()!=total {return Err("telemetry incomplete count summary".into());}
    let mut ordered=parts.iter().collect::<Vec<_>>();ordered.sort_by_key(|part|part["part_index"].as_u64());
    let mut base=first.clone();for key in FIELDS {base.as_object_mut().unwrap().remove(*key);}base.as_object_mut().unwrap().remove("counts");
    let mut counts=Map::new();let mut bytes=0;
    for (index,part) in ordered.into_iter().enumerate() {
        if part["group_id"]!=first["group_id"]||part["group_hash"]!=first["group_hash"]||part["part_count"]!=first["part_count"]||part["part_index"]!=json!(index) {return Err("telemetry inconsistent count summary parts".into());}
        let mut metadata=part.clone();for key in FIELDS {metadata.as_object_mut().unwrap().remove(*key);}metadata.as_object_mut().unwrap().remove("counts");
        if metadata!=base {return Err("telemetry inconsistent count summary metadata".into());}
        for (key,count) in part["counts"].as_object().ok_or("telemetry invalid count summary")? {
            bytes+=serde_json::to_string(key).map_err(super::err)?.len()+count.to_string().len()+2;
            if bytes>BYTES {return Err("telemetry invalid assembled summary size".into());}
            if counts.insert(key.clone(),count.clone()).is_some() {return Err("telemetry repeated count summary key".into());}
        }
    }
    base["counts"]=Value::Object(counts);let payload=canonical(&mut base);
    if payload.len()>BYTES||crate::sha256_hex(payload.as_bytes())!=first["group_hash"].as_str().unwrap() {return Err("telemetry count summary digest mismatch".into());}
    super::validate(&base,None)?;Ok(base)
}

/// Bounded assembly across export pages; producers append all parts contiguously in one batch.
#[derive(Default)]
pub struct Assembler {pending:Vec<Value>,failed:bool}
impl Assembler {
    pub fn push(&mut self,value: Value) -> Res<Option<Value>> {
        if self.failed {return Err("telemetry count summary reader failed".into());}
        let result=self.push_validated(value);if result.is_err() {self.failed=true;self.pending.clear();}result
    }
    fn push_validated(&mut self,value: Value) -> Res<Option<Value>> {
        super::validate(&value,None)?;
        if value.to_string().len()>EVENT_BYTES {return Err("telemetry event too large".into());}
        if value.get("group_id").is_none() {
            if !self.pending.is_empty() {return Err("telemetry incomplete count summary".into());}
            return Ok(Some(value));
        }
        if self.pending.first().is_some_and(|first|first["group_id"]!=value["group_id"]) {return Err("telemetry incomplete count summary".into());}
        let total=value["part_count"].as_u64().unwrap() as usize;self.pending.push(value);
        if self.pending.len()>total {return Err("telemetry invalid count summary part count".into());}
        if self.pending.len()<total {return Ok(None);}
        let result=assemble(&self.pending);self.pending.clear();result.map(Some)
    }
    pub fn finish(self) -> Res<()> {
        if self.failed||!self.pending.is_empty() {return Err("telemetry incomplete count summary".into());}Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::Store;
    fn value(count: usize) -> Value {
        let counts=(0..count).map(|i|(format!("GET:issues.N.route{i:05}"),json!(u64::MAX))).collect::<Map<_,_>>();
        json!({"product":"test","version":"1","operation":"requests.summary","kind":"requests","trace":"trace","span":"span","started_ms":1,"outcome":"unknown","error":"none","counts":counts})
    }
    fn values(events: &[Event]) -> Vec<Value> {events.iter().map(|event|serde_json::from_str(&event.payload).unwrap()).collect()}
    #[test]
    fn bounded_parts_preserve_unsigned_counts_and_stable_identity_through_import_reopen_and_replay() {
        let logical=value(1800);assert!(logical.to_string().len()>64<<10);let events=Event::split_identified("archive","position.1",logical.clone()).unwrap();
        assert!((2..=PARTS).contains(&events.len()));assert!(events.iter().all(|event|event.payload.len()<=EVENT_BYTES));
        assert_eq!(events[0].id,super::super::identity("archive","position.1").unwrap());
        assert_eq!(events.iter().map(Event::id).collect::<Vec<_>>(),Event::split_identified("archive","position.1",logical.clone()).unwrap().iter().map(Event::id).collect::<Vec<_>>());
        let mut parts=values(&events);parts.reverse();assert_eq!(assemble(&parts).unwrap(),logical);
        let dir=crate::scratch("telemetry-count-summary");let path=dir.join("db");let store=Store::open(&path).unwrap();let receipt=super::super::clock().unwrap()-1000;
        store.import_history("archive",None,"position.1",&events,&vec![receipt;events.len()]).unwrap();drop(store);
        let store=Store::open(&path).unwrap();let before=store.export(0,0,1024).unwrap();let restored=before["events"].as_array().unwrap().iter().map(|row|{assert_eq!(row["received_ms"],receipt);row["observation"].clone()}).collect::<Vec<_>>();assert_eq!(assemble(&restored).unwrap(),logical);
        store.import_history("archive",None,"position.1",&events,&vec![receipt;events.len()]).unwrap();assert_eq!(store.export(0,0,1024).unwrap(),before);
        let mut changed=logical;changed["counts"]["GET:issues.N.route00000"]=json!(0);let changed=Event::split_identified("archive","position.1",changed).unwrap();assert!(store.append(&changed).unwrap_err().contains("immutable id"));
        let small=Event::split_identified("archive","position.1",value(1)).unwrap();assert_eq!(small.len(),1);assert!(store.append(&small).unwrap_err().contains("immutable id"));assert_eq!(store.export(0,0,1024).unwrap(),before);
        drop(store);std::fs::remove_dir_all(dir).unwrap();
    }
    #[test]
    fn assembly_refuses_missing_repeated_mixed_and_mutated_parts() {
        let original=Event::split(value(1000)).unwrap();let parts=values(&original);assert!(parts.len()>1);
        assert!(assemble(&parts[..parts.len()-1]).is_err());let mut bad=parts.clone();bad[1]=bad[0].clone();assert!(assemble(&bad).is_err());
        let mut bad=parts.clone();bad[1]["source"]=json!("other");assert!(assemble(&bad).is_err());
        let mut bad=parts.clone();let key=bad[0]["counts"].as_object().unwrap().keys().next().unwrap().clone();bad[0]["counts"][&key]=json!(0);assert!(assemble(&bad).unwrap_err().contains("digest"));
        let mut bad=parts.clone();bad[1]["group_id"]=json!("0".repeat(32));assert!(assemble(&bad).is_err());
        let mut bad=parts.clone();bad[1]["counts"][&key]=json!(1);assert!(assemble(&bad).is_err());
        for (name,bad_value) in [("part_count",json!(1)),("part_index",json!(16)),("group_id",json!("unsafe/path")),("group_hash",json!("short"))] {let mut bad=parts[0].clone();bad[name]=bad_value;assert!(Event::new(bad).is_err());}
        assert!(Event::split(parts[0].clone()).is_err());assert!(assemble(&[]).is_err());assert!(assemble(&vec![parts[0].clone();PARTS+1]).is_err());
    }
    #[test]
    fn streaming_pages_keep_bounded_state_and_latch_incomplete_groups_as_unknown() {
        let logical=value(1000);let parts=values(&Event::split(logical.clone()).unwrap());let mut reader=Assembler::default();
        for part in &parts[..parts.len()-1] {assert!(reader.push(part.clone()).unwrap().is_none());assert!(reader.pending.len()<PARTS);}
        assert_eq!(reader.push(parts.last().unwrap().clone()).unwrap(),Some(logical));assert!(reader.pending.is_empty());reader.finish().unwrap();
        let mut incomplete=Assembler::default();incomplete.push(parts[0].clone()).unwrap();assert!(incomplete.finish().is_err());
        let mut failed=Assembler::default();failed.push(parts[0].clone()).unwrap();assert!(failed.push(value(1)).is_err());assert!(failed.push(value(1)).is_err());assert!(failed.finish().is_err());
        let mut complete=Assembler::default();assert_eq!(complete.push(value(1)).unwrap(),Some(value(1)));complete.finish().unwrap();
    }
    #[test]
    fn age_logical_and_physical_removal_keep_summary_groups_whole() {
        for mode in ["age","logical","physical-selection"] {
            let dir=crate::scratch("telemetry-summary-prune");let path=dir.join("db");let store=Store::open(&path).unwrap();let parts=Event::split(value(1000)).unwrap();store.append(&parts).unwrap();let tail=Event::new(value(1)).unwrap();store.append(&[tail.clone()]).unwrap();
            {let mut db=store.db.lock().unwrap();let tx=db.transaction().unwrap();let now=super::super::clock().unwrap();
            match mode {
                "age"=>{tx.execute("UPDATE events SET received_ms=?1 WHERE id=?2",rusqlite::params![now-super::super::AGE_MS-1,parts[0].id]).unwrap();super::super::prune(&tx,now,super::super::AGE_MS,super::super::PAYLOAD_BYTES).unwrap();},
                "logical"=>{let total:i64=tx.query_row("SELECT retained_bytes FROM metadata",[],|r|r.get(0)).unwrap();super::super::prune(&tx,now,super::super::AGE_MS,total-parts[0].payload.len() as i64).unwrap();},
                _=>{let rows=super::super::read_rows(&tx,&format!("SELECT {} FROM events WHERE id=?1",super::super::SELECT_ROW),[&parts.last().unwrap().id],&mut super::super::Cache::default()).unwrap();assert_eq!(super::super::remove_rows(&tx,rows).unwrap(),parts.len());},
            }tx.commit().unwrap();}
            let report=store.export(0,0,1024).unwrap();assert_eq!(report["events"].as_array().unwrap().len(),1,"{mode}");assert_eq!(report["events"][0]["id"],tail.id);assert_eq!(report["pruned_count"],parts.len());drop(store);std::fs::remove_dir_all(dir).unwrap();
        }
    }
    #[test]
    fn summary_removal_never_deletes_an_unrelated_observation_with_the_base_id() {
        let dir=crate::scratch("telemetry-summary-prune-membership");let store=Store::open(&dir.join("db")).unwrap();let normal=Event::new(value(1)).unwrap();store.append(&[normal.clone()]).unwrap();
        let parts=split(value(1000),normal.id()).unwrap();store.append(&parts[1..]).unwrap();
        store.db.lock().unwrap().execute("UPDATE events SET received_ms=0 WHERE id=?1",[parts[1].id()]).unwrap();store.prune().unwrap();
        let rows=store.export(0,0,1024).unwrap();assert_eq!(rows["events"].as_array().unwrap().len(),1);assert_eq!(rows["events"][0]["id"],normal.id);drop(store);std::fs::remove_dir_all(dir).unwrap();
    }
    #[test]
    fn part_identity_is_bound_to_group_and_failed_removal_rolls_back() {
        let dir=crate::scratch("telemetry-summary-prune-rollback");let store=Store::open(&dir.join("db")).unwrap();let parts=Event::split(value(1000)).unwrap();let part=values(&parts)[0].clone();assert_eq!(Event::new(part.clone()).unwrap().id,parts[0].id);assert!(Event::with_id(part,Some(&"0".repeat(32))).is_err());store.append(&parts).unwrap();let before=store.export(0,0,1024).unwrap();
        {let mut db=store.db.lock().unwrap();db.execute_batch(&format!("CREATE TRIGGER refuse_group_delete BEFORE DELETE ON events WHEN old.id='{}' BEGIN SELECT RAISE(ABORT,'injected summary prune failure'); END",parts.last().unwrap().id)).unwrap();let tx=db.transaction().unwrap();assert!(super::super::prune(&tx,super::super::clock().unwrap(),super::super::AGE_MS,0).unwrap_err().contains("injected summary prune failure"));}
        assert_eq!(store.export(0,0,1024).unwrap(),before);drop(store);std::fs::remove_dir_all(dir).unwrap();
    }
    #[test]
    fn normal_ids_payloads_and_limits_remain_compatible() {
        let logical=value(1);let ordinary=Event::identified("archive","position.1",logical.clone()).unwrap();let parts=Event::split_identified("archive","position.1",logical.clone()).unwrap();assert_eq!(parts.len(),1);assert_eq!(parts[0].id,ordinary.id);assert_eq!(parts[0].payload,ordinary.payload);assert_eq!(assemble(&values(&parts)).unwrap(),logical);
        assert!(Event::split_identified("bad/path","key",value(1)).is_err());assert!(Event::split(value(4000)).is_err());
        let mut bad=value(1000);bad["counts"]["unsafe/path"]=json!(1);assert!(Event::split(bad).is_err());
    }
}
