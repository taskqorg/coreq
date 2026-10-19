//! Lossless internal blocks; metadata validation stays at Event and store boundaries.
use super::{BATCH, EVENT_BYTES, Res};
use flate2::{Compression, Decompress, FlushDecompress, Status, write::ZlibEncoder};
use std::{io::Write, ops::Range};
pub(super) const RAW_BYTES: usize = 1024 * 1024;
pub(super) const ENCODED_BYTES: usize = RAW_BYTES + 65536 + 12;
const BAD: &str = "telemetry corrupt compressed block";

pub(super) fn groups(payloads: &[&str]) -> Res<Vec<Range<usize>>> {
    if payloads.len()>BATCH {return Err("telemetry block count too large".into());}
    let mut groups=Vec::new();let mut start=0;let mut bytes=0;
    for (i,payload) in payloads.iter().enumerate() {
        if payload.is_empty()||payload.len()>EVENT_BYTES {return Err("telemetry invalid block observation size".into());}
        if bytes+4+payload.len()>RAW_BYTES {groups.push(start..i);start=i;bytes=0;}
        bytes+=4+payload.len();
    }
    if start<payloads.len() {groups.push(start..payloads.len());}Ok(groups)
}
pub(super) fn encode(payloads: &[&str]) -> Res<Vec<u8>> {
    let partitions=groups(payloads)?;
    if partitions.len()!=1 {return Err("telemetry invalid block size".into());}
    let mut frame=Vec::new();
    for payload in payloads {frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());frame.extend_from_slice(payload.as_bytes());}
    let mut encoder=ZlibEncoder::new(Vec::new(),Compression::fast());
    encoder.write_all(&frame).map_err(|_|"telemetry block encoding failed")?;
    let compressed=encoder.finish().map_err(|_|"telemetry block encoding failed")?;
    let mut out=Vec::with_capacity(12+compressed.len());out.extend_from_slice(b"CQB1");
    out.extend_from_slice(&(payloads.len() as u32).to_be_bytes());out.extend_from_slice(&(frame.len() as u32).to_be_bytes());out.extend(compressed);
    if out.len()>ENCODED_BYTES {return Err("telemetry encoded block too large".into());}Ok(out)
}
pub(super) fn decode(bytes: &[u8]) -> Res<Vec<String>> {
    if bytes.len()<12||bytes.len()>ENCODED_BYTES||&bytes[..4]!=b"CQB1" {return Err(BAD.into());}
    let count=u32::from_be_bytes(bytes[4..8].try_into().unwrap()) as usize;
    let size=u32::from_be_bytes(bytes[8..12].try_into().unwrap()) as usize;
    if !(1..=BATCH).contains(&count)||size>RAW_BYTES||size<count*5||size>count*(EVENT_BYTES+4) {return Err(BAD.into());}
    let mut decoded=vec![0;size+1];let mut decoder=Decompress::new(true);
    let status=decoder.decompress(&bytes[12..],&mut decoded,FlushDecompress::Finish).map_err(|_|BAD)?;
    if status!=Status::StreamEnd||decoder.total_in()!=bytes.len() as u64-12||decoder.total_out()!=size as u64 {return Err(BAD.into());}
    let mut position=0;let mut payloads=Vec::with_capacity(count);
    for _ in 0..count {
        let length=decoded.get(position..position+4).ok_or(BAD)?;position+=4;
        let length=u32::from_be_bytes(length.try_into().unwrap()) as usize;
        if !(1..=EVENT_BYTES).contains(&length)||position+length>size {return Err(BAD.into());}
        let payload=std::str::from_utf8(&decoded[position..position+length]).map_err(|_|BAD)?;position+=length;
        payloads.push(payload.to_owned());
    }
    if position!=size {return Err(BAD.into());}Ok(payloads)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn exact_roundtrip_splits_by_bytes_and_refuses_corruption() {
        let payload="{\"unknown\":null,\"text\":\"тест\\n\"}".to_owned();
        let original=(0..BATCH).map(|n|if n%2==0 {payload.as_str()} else {" {\n \"unknown\": null } \n"}).collect::<Vec<_>>();let encoded=encode(&original).unwrap();
        assert_eq!(decode(&encoded).unwrap(),original);
        let mut trailing=encoded.clone();trailing.push(0);assert!(decode(&trailing).is_err());
        for cut in 0..encoded.len() {assert!(decode(&encoded[..cut]).is_err());}
        let mut corrupt=encoded.clone();let n=corrupt.len()-1;corrupt[n]^=1;assert!(decode(&corrupt).is_err());
        let mut oversized=encoded.clone();oversized[8..12].copy_from_slice(&(RAW_BYTES as u32+1).to_be_bytes());assert!(decode(&oversized).is_err());
        let mut wrong_count=encoded.clone();wrong_count[4..8].copy_from_slice(&1u32.to_be_bytes());assert!(decode(&wrong_count).is_err());
        assert!(encode(&[]).is_err());assert!(encode(&[""]).is_err());
        let large="x".repeat(EVENT_BYTES);let values=vec![large.as_str();BATCH];let groups=groups(&values).unwrap();assert!(groups.len()>1);
        for group in groups {assert_eq!(decode(&encode(&values[group.clone()]).unwrap()).unwrap(),values[group]);}
    }
    #[test]
    fn valid_checksums_cannot_bypass_framing_utf8_or_expansion_bounds() {
        for frame in [&[0,0,0,1,255][..],&[0,0,0,2,b'x'][..],&[0,0,0,1,b'x',0][..]] {
            let mut encoder=ZlibEncoder::new(Vec::new(),Compression::fast());encoder.write_all(frame).unwrap();
            let mut bytes=b"CQB1".to_vec();bytes.extend_from_slice(&1u32.to_be_bytes());bytes.extend_from_slice(&(frame.len() as u32).to_be_bytes());bytes.extend(encoder.finish().unwrap());
            assert!(decode(&bytes).is_err());
        }
        let large="x".repeat(EVENT_BYTES);let mut bytes=encode(&[&large]).unwrap();
        bytes[8..12].copy_from_slice(&5u32.to_be_bytes());assert!(decode(&bytes).is_err());
        let mut bytes=encode(&["valid"]).unwrap();bytes[..4].copy_from_slice(b"CQB2");assert!(decode(&bytes).is_err());
    }
}
