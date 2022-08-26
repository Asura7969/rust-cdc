use bytes::{Buf, Bytes};
use rustcdc::*;
use rustcdc::mysql::{Event, FormatDescriptionEventData, TableMap, TableMapEventData};
use rustcdc::io::buf::BufExt;

#[test]
fn test_format_desc() {
    let input = include_bytes!("events/15_format_desc/log.bin").to_vec();
    let mut table_map = TableMap::default();
    let mut buf = Bytes::from(input);
    buf.get_bytes(4);
    // buf.split_off(20); // header
    // let event = FormatDescriptionEventData::decode_with(buf);
    let event = Event::decode(buf, &mut table_map).unwrap();

    match event.data.as_any().downcast_ref::<FormatDescriptionEventData>() {
    // match event {
        Some(fde) => {
            assert_eq!(fde.binlog_version, 4);
            assert_eq!(fde.sever_version, "5.7.30-log");
            assert_eq!(fde.create_timestamp, 1596175634)
        },

        _ => panic!("should be format desc"),
    }
}


