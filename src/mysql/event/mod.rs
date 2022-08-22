mod xid_event;
mod query_event;
mod format_des_event;


pub(crate) use format_des_event::FormatDescriptionEvent;
use std::any::Any;
use std::borrow::Borrow;
use std::marker::PhantomData;
use std::sync::Arc;
use bytes::{Buf, Bytes, BytesMut};
use crate::err_protocol;
use crate::error::Error;
use crate::io::Decode;

pub(crate) struct Event<T: EventData> {
    header: EventHeaderV4,
    data: T,
}

impl <T> Event<T>
where T: EventData {

    pub(crate) fn decode(mut buf: Bytes) -> Result<Self, Error> {
        /// [header size is 19]
        ///
        /// [header size is 19]: https://dev.mysql.com/doc/internals/en/binlog-event-header.html
        let mut header_bytes = buf.split_off(20);
        let header = EventHeaderV4::decode(header_bytes)?;

        let data = match header.event_type {
            EventType::FormatDescriptionEvent => {
                /// [Replication event checksums]
                ///
                /// [Replication event checksums]: https://dev.mysql.com/worklog/task/?id=2540#tabs-2540-4
                ///
                /// ---
                ///
                /// +-----------+------------+-----------+------------------------+----------+
                /// | Header    | Payload (dataLength)   | Checksum Type (1 byte) | Checksum |
                /// +-----------+------------+-----------+------------------------+----------+
                ///             |                    (eventBodyLength)                       |
                ///             +------------------------------------------------------------+
                FormatDescriptionEvent::decode_with(buf)?

                // unimplemented!()
            },
            EventType::TableMapEvent => {
                unimplemented!()
            },
            _ => {
                unimplemented!()
            },
        };
        // Ok(Self{header, data})
        unimplemented!()
    }
}

// pub(crate) trait EventHeader<'a>: Decode<'a> {
//     fn get_timestamp(&self) -> u32;
//     fn get_event_type(&self) -> &EventType;
//     fn get_server_id(&self) -> u32;
//     fn get_event_size(&self) -> u32;
//     fn get_log_pos(&self) -> u32;
//     fn get_flags(&self) -> u16;
// }

pub(crate) struct EventHeaderV4 {
    pub(crate) timestamp: u32,
    pub(crate) event_type: EventType,
    pub(crate) server_id: u32,
    pub(crate) event_size: u32,
    pub(crate) log_pos: u32,
    pub(crate) flags: u16,
}

impl EventHeaderV4 {
    pub(crate) fn decode(mut buf: Bytes) -> Result<Self, Error> {
        Ok(Self {
            timestamp: buf.get_u32_le(),
            event_type: EventType::try_from_u8(buf.get_u8())?,
            server_id: buf.get_u32_le(),
            event_size: buf.get_u32_le(),
            log_pos: buf.get_u32_le(),
            flags: buf.get_u16_le(),
        })
    }
}

// impl EventHeader<'_> for EventHeaderV4 {
//     fn get_timestamp(&self) -> u32 {
//         self.timestamp
//     }
//
//     fn get_event_type(&self) -> &EventType {
//         self.event_type.borrow()
//     }
//
//     fn get_server_id(&self) -> u32 {
//         self.server_id
//     }
//
//     fn get_event_size(&self) -> u32 {
//         self.event_size
//     }
//
//     fn get_log_pos(&self) -> u32 {
//         self.log_pos
//     }
//
//     fn get_flags(&self) -> u16 {
//         self.flags
//     }
// }

pub(crate) trait EventData {

}

// https://dev.mysql.com/doc/internals/en/event-classes-and-types.html
// https://dev.mysql.com/doc/internals/en/event-data-for-specific-event-types.html
// https://dev.mysql.com/doc/internals/en/binlog-event-type.html
#[derive(Debug, Copy, Clone, PartialEq)]
#[repr(u8)]
pub(crate) enum EventType {
    UnknownEvent = 0x00,
    StartEventV3 = 0x01,
    QueryEvent = 0x02,
    StopEvent = 0x03,
    RotateEvent = 0x04,
    IntvarEvent = 0x05,
    LoadEvent = 0x06,
    SlaveEvent = 0x07,
    CreateFileEvent = 0x08,
    AppendBlockEvent = 0x09,
    ExecLoadEvent = 0x0a,
    DeleteFileEvent = 0x0b,
    NewLoadEvent = 0x0c,
    RanDEvent = 0x0d,
    UserVarEvent = 0x0e,
    FormatDescriptionEvent = 0x0f,
    XidEvent = 0x10,
    BeginLoadQueryEvent = 0x11,
    ExecuteLoadQueryEvent = 0x12,
    TableMapEvent = 0x13,
    WriteRowsEventV0 = 0x14,
    UpdateRowsEventV0 = 0x15,
    DeleteRowsEventV0 = 0x16,
    WriteRowsEventV1 = 0x17,
    UpdateRowsEventV1 = 0x18,
    DeleteRowsEventV1 = 0x19,
    IncidentEvent = 0x1a,
    HeartbeatEvent = 0x1b,
    IgnorableEvent = 0x1c,
    RowsQueryEvent = 0x1d,
    WriteRowsEventV2 = 0x1e,
    UpdateRowsEventV2 = 0x1f,
    DeleteRowsEventV2 = 0x20,
    GtidEvent = 0x21,
    AnonymousGtidEvent = 0x22,
    PreviousGtidsEvent = 0x23,
}

impl EventType {
    pub(crate) fn try_from_u8(id: u8) -> Result<Self, Error> {
        Ok(match id {
            0x00 => EventType::UnknownEvent,
            0x01 => EventType::StartEventV3,
            0x02 => EventType::QueryEvent,
            0x03 => EventType::StopEvent,
            0x04 => EventType::RotateEvent,
            0x05 => EventType::IntvarEvent,
            0x06 => EventType::LoadEvent,
            0x07 => EventType::SlaveEvent,
            0x08 => EventType::CreateFileEvent,
            0x09 => EventType::AppendBlockEvent,
            0x0a => EventType::ExecLoadEvent,
            0x0b => EventType::DeleteFileEvent,
            0x0c => EventType::NewLoadEvent,
            0x0d => EventType::RanDEvent,
            0x0e => EventType::UserVarEvent,
            0x0f => EventType::FormatDescriptionEvent,
            0x10 => EventType::XidEvent,
            0x11 => EventType::BeginLoadQueryEvent,
            0x12 => EventType::ExecuteLoadQueryEvent,
            0x13 => EventType::TableMapEvent,
            0x14 => EventType::WriteRowsEventV0,
            0x15 => EventType::UpdateRowsEventV0,
            0x16 => EventType::DeleteRowsEventV0,
            0x17 => EventType::WriteRowsEventV1,
            0x18 => EventType::UpdateRowsEventV1,
            0x19 => EventType::DeleteRowsEventV1,
            0x1a => EventType::IncidentEvent,
            0x1b => EventType::HeartbeatEvent,
            0x1c => EventType::IgnorableEvent,
            0x1d => EventType::RowsQueryEvent,
            0x1e => EventType::WriteRowsEventV2,
            0x1f => EventType::UpdateRowsEventV2,
            0x20 => EventType::DeleteRowsEventV2,
            0x21 => EventType::GtidEvent,
            0x22 => EventType::AnonymousGtidEvent,
            0x23 => EventType::PreviousGtidsEvent,

            _ => {
                return Err(err_protocol!("unknown event type 0x{:02x}", id));
            }
        })
    }

    pub(crate) fn is_row_mutation(event_type: EventType) -> bool {
        EventType::is_write(event_type) ||
            EventType::is_update(event_type) ||
            EventType::is_delete(event_type)
    }

    pub(crate) fn is_write(event_type: EventType) -> bool {
        event_type.eq(&EventType::WriteRowsEventV0) ||
            event_type.eq(&EventType::WriteRowsEventV1) ||
            event_type.eq(&EventType::WriteRowsEventV2)
    }

    pub(crate) fn is_update(event_type: EventType) -> bool {
        event_type.eq(&EventType::UpdateRowsEventV0) ||
            event_type.eq(&EventType::UpdateRowsEventV1) ||
            event_type.eq(&EventType::UpdateRowsEventV2)
    }

    pub(crate) fn is_delete(event_type: EventType) -> bool {
        event_type.eq(&EventType::DeleteRowsEventV0) ||
            event_type.eq(&EventType::DeleteRowsEventV1) ||
            event_type.eq(&EventType::DeleteRowsEventV2)
    }
}
