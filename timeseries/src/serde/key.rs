// Key structures with big-endian encoding

use super::*;
use crate::model::{BucketSize, BucketStart, SeriesFingerprint, SeriesId};
use bytes::{Bytes, BytesMut};
use common::BytesRange;
use common::serde::key_prefix::KeyPrefix;

/// Encodes a string as a tuple element for inverted index keys.
///
/// This uses a FoundationDB-style tuple encoding with a `0x00` delimiter
/// to preserve lexicographical ordering for range scans. This helper is
/// intentionally scoped to inverted index key encoding and does not replace
/// shared UTF-8 encoding utilities.
fn encode_tuple_string(buf: &mut Vec<u8>, s: &str) {
    for b in s.as_bytes() {
        if *b == 0x00 {
            // Escape null bytes
            buf.push(0x00);
            buf.push(0xFF);
        } else {
            buf.push(*b);
        }
    }
    // Tuple element terminator
    buf.push(0x00);
}

/// Decodes a tuple-encoded string element from inverted index keys.
///
/// This is the inverse of `encode_tuple_string`. It reads bytes until a
/// `0x00` terminator is found, unescaping `0x00 0xFF` sequences back to
/// literal `0x00` bytes. This helper is intentionally scoped to inverted
/// index key decoding and does not replace shared UTF-8 decoding utilities.
fn decode_tuple_string(buf: &mut &[u8]) -> Result<String, EncodingError> {
    let mut bytes = Vec::new();
    let mut i = 0;

    while i < buf.len() {
        let b = buf[i];
        if b == 0x00 {
            // Check if this is an escape sequence or terminator
            if i + 1 < buf.len() && buf[i + 1] == 0xFF {
                // Escaped null byte
                bytes.push(0x00);
                i += 2;
            } else {
                // Terminator found
                *buf = &buf[i + 1..];
                return String::from_utf8(bytes).map_err(|e| EncodingError {
                    message: format!("Invalid UTF-8 in tuple string: {}", e),
                });
            }
        } else {
            bytes.push(b);
            i += 1;
        }
    }

    Err(EncodingError {
        message: "Missing terminator in tuple-encoded string".to_string(),
    })
}

/// BucketList key (global-scoped)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BucketListKey;

impl BucketListKey {
    pub fn encode(&self) -> Bytes {
        RecordType::BucketList.prefix().to_bytes()
    }

    pub fn decode(buf: &[u8]) -> Result<Self, EncodingError> {
        let prefix = KeyPrefix::from_bytes_versioned(buf, KEY_VERSION)?;
        let record_type = record_type_from_tag(prefix.tag())?;
        if record_type != RecordType::BucketList {
            return Err(EncodingError {
                message: format!(
                    "invalid record type: expected BucketList, got {:?}",
                    record_type
                ),
            });
        }
        if bucket_size_from_tag(prefix.tag()).is_some() {
            return Err(EncodingError {
                message: "BucketListKey should be global-scoped (bucket_size should be None)"
                    .to_string(),
            });
        }
        Ok(BucketListKey)
    }
}

/// SeriesDictionary key
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SeriesDictionaryKey {
    pub time_bucket: BucketStart,
    pub bucket_size: BucketSize,
    pub series_fingerprint: SeriesFingerprint,
}

impl SeriesDictionaryKey {
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::new();
        RecordType::SeriesDictionary
            .prefix_with_bucket_size(self.bucket_size)
            .write_to(&mut buf);
        buf.extend_from_slice(&self.time_bucket.to_be_bytes());
        buf.extend_from_slice(&self.series_fingerprint.to_be_bytes());
        buf.freeze()
    }

    pub fn decode(buf: &[u8]) -> Result<Self, EncodingError> {
        if buf.len() < 2 + 4 + 16 {
            return Err(EncodingError {
                message: "Buffer too short for SeriesDictionaryKey".to_string(),
            });
        }
        let prefix = KeyPrefix::from_bytes_versioned(buf, KEY_VERSION)?;
        let record_type = record_type_from_tag(prefix.tag())?;
        if record_type != RecordType::SeriesDictionary {
            return Err(EncodingError {
                message: format!(
                    "invalid record type: expected SeriesDictionary, got {:?}",
                    record_type
                ),
            });
        }
        let bucket_size = bucket_size_from_tag(prefix.tag()).ok_or_else(|| EncodingError {
            message: "SeriesDictionaryKey should be bucket-scoped".to_string(),
        })?;

        let time_bucket = u32::from_be_bytes([buf[2], buf[3], buf[4], buf[5]]);
        let series_fingerprint = u128::from_be_bytes([
            buf[6], buf[7], buf[8], buf[9], buf[10], buf[11], buf[12], buf[13], buf[14], buf[15],
            buf[16], buf[17], buf[18], buf[19], buf[20], buf[21],
        ]);

        Ok(SeriesDictionaryKey {
            time_bucket,
            series_fingerprint,
            bucket_size,
        })
    }
}

impl RecordKey for SeriesDictionaryKey {
    const RECORD_TYPE: RecordType = RecordType::SeriesDictionary;
}

impl TimeBucketScoped for SeriesDictionaryKey {
    fn bucket(&self) -> crate::model::TimeBucket {
        crate::model::TimeBucket {
            start: self.time_bucket,
            size: self.bucket_size,
        }
    }
}

/// ForwardIndex key
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForwardIndexKey {
    pub time_bucket: BucketStart,
    pub bucket_size: BucketSize,
    pub series_id: SeriesId,
}

impl ForwardIndexKey {
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::new();
        RecordType::ForwardIndex
            .prefix_with_bucket_size(self.bucket_size)
            .write_to(&mut buf);
        buf.extend_from_slice(&self.time_bucket.to_be_bytes());
        buf.extend_from_slice(&self.series_id.to_be_bytes());
        buf.freeze()
    }

    pub fn decode(buf: &[u8]) -> Result<Self, EncodingError> {
        if buf.len() < 2 + 4 + 4 {
            return Err(EncodingError {
                message: "Buffer too short for ForwardIndexKey".to_string(),
            });
        }
        let prefix = KeyPrefix::from_bytes_versioned(buf, KEY_VERSION)?;
        let record_type = record_type_from_tag(prefix.tag())?;
        if record_type != RecordType::ForwardIndex {
            return Err(EncodingError {
                message: format!(
                    "invalid record type: expected ForwardIndex, got {:?}",
                    record_type
                ),
            });
        }
        let bucket_size = bucket_size_from_tag(prefix.tag()).ok_or_else(|| EncodingError {
            message: "ForwardIndexKey should be bucket-scoped".to_string(),
        })?;

        let time_bucket = u32::from_be_bytes([buf[2], buf[3], buf[4], buf[5]]);
        let series_id = u32::from_be_bytes([buf[6], buf[7], buf[8], buf[9]]);

        Ok(ForwardIndexKey {
            time_bucket,
            series_id,
            bucket_size,
        })
    }
}

impl RecordKey for ForwardIndexKey {
    const RECORD_TYPE: RecordType = RecordType::ForwardIndex;
}

impl TimeBucketScoped for ForwardIndexKey {
    fn bucket(&self) -> crate::model::TimeBucket {
        crate::model::TimeBucket {
            start: self.time_bucket,
            size: self.bucket_size,
        }
    }
}

/// InvertedIndex key
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvertedIndexKey {
    pub time_bucket: BucketStart,
    pub bucket_size: BucketSize,
    pub attribute: String,
    pub value: String,
}

impl InvertedIndexKey {
    pub fn encode(&self) -> Bytes {
        let mut buf = Vec::new();
        let mut prefix_buf = BytesMut::new();
        RecordType::InvertedIndex
            .prefix_with_bucket_size(self.bucket_size)
            .write_to(&mut prefix_buf);
        buf.extend_from_slice(&prefix_buf);
        buf.extend_from_slice(&self.time_bucket.to_be_bytes());
        encode_tuple_string(&mut buf, &self.attribute);
        encode_tuple_string(&mut buf, &self.value);
        Bytes::from(buf)
    }

    /// Create a BytesRange that covers all entries for a specific attribute (label name)
    /// within a given bucket. This allows efficient scanning for all values of a label.
    pub fn attribute_range(bucket: &crate::model::TimeBucket, attribute: &str) -> BytesRange {
        let mut buf = Vec::new();
        let mut prefix_buf = BytesMut::new();
        RecordType::InvertedIndex
            .prefix_with_bucket_size(bucket.size)
            .write_to(&mut prefix_buf);
        buf.extend_from_slice(&prefix_buf);
        buf.extend_from_slice(&bucket.start.to_be_bytes());
        encode_tuple_string(&mut buf, attribute);
        BytesRange::prefix(Bytes::from(buf))
    }

    pub fn decode(buf: &[u8]) -> Result<Self, EncodingError> {
        if buf.len() < 2 + 4 {
            return Err(EncodingError {
                message: "Buffer too short for InvertedIndexKey".to_string(),
            });
        }
        let prefix = KeyPrefix::from_bytes_versioned(buf, KEY_VERSION)?;
        let record_type = record_type_from_tag(prefix.tag())?;
        if record_type != RecordType::InvertedIndex {
            return Err(EncodingError {
                message: format!(
                    "invalid record type: expected InvertedIndex, got {:?}",
                    record_type
                ),
            });
        }
        let bucket_size = bucket_size_from_tag(prefix.tag()).ok_or_else(|| EncodingError {
            message: "InvertedIndexKey should be bucket-scoped".to_string(),
        })?;

        let mut slice = &buf[2..];
        let time_bucket = u32::from_be_bytes([slice[0], slice[1], slice[2], slice[3]]);
        slice = &slice[4..];

        let attribute = decode_tuple_string(&mut slice)?;
        let value = decode_tuple_string(&mut slice)?;

        Ok(InvertedIndexKey {
            time_bucket,
            attribute,
            value,
            bucket_size,
        })
    }
}

impl RecordKey for InvertedIndexKey {
    const RECORD_TYPE: RecordType = RecordType::InvertedIndex;
}

impl TimeBucketScoped for InvertedIndexKey {
    fn bucket(&self) -> crate::model::TimeBucket {
        crate::model::TimeBucket {
            start: self.time_bucket,
            size: self.bucket_size,
        }
    }
}

/// TimeSeries key
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimeSeriesKey {
    pub time_bucket: BucketStart,
    pub bucket_size: BucketSize,
    pub series_id: SeriesId,
}

impl TimeSeriesKey {
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::new();
        RecordType::TimeSeries
            .prefix_with_bucket_size(self.bucket_size)
            .write_to(&mut buf);
        buf.extend_from_slice(&self.time_bucket.to_be_bytes());
        buf.extend_from_slice(&self.series_id.to_be_bytes());
        buf.freeze()
    }

    pub fn decode(buf: &[u8]) -> Result<Self, EncodingError> {
        if buf.len() < 2 + 4 + 4 {
            return Err(EncodingError {
                message: "Buffer too short for TimeSeriesKey".to_string(),
            });
        }
        let prefix = KeyPrefix::from_bytes_versioned(buf, KEY_VERSION)?;
        let record_type = record_type_from_tag(prefix.tag())?;
        if record_type != RecordType::TimeSeries {
            return Err(EncodingError {
                message: format!(
                    "invalid record type: expected TimeSeries, got {:?}",
                    record_type
                ),
            });
        }
        let bucket_size = bucket_size_from_tag(prefix.tag()).ok_or_else(|| EncodingError {
            message: "TimeSeriesKey should be bucket-scoped".to_string(),
        })?;

        let time_bucket = u32::from_be_bytes([buf[2], buf[3], buf[4], buf[5]]);
        let series_id = u32::from_be_bytes([buf[6], buf[7], buf[8], buf[9]]);

        Ok(TimeSeriesKey {
            time_bucket,
            series_id,
            bucket_size,
        })
    }
}

impl RecordKey for TimeSeriesKey {
    const RECORD_TYPE: RecordType = RecordType::TimeSeries;
}

impl TimeBucketScoped for TimeSeriesKey {
    fn bucket(&self) -> crate::model::TimeBucket {
        crate::model::TimeBucket {
            start: self.time_bucket,
            size: self.bucket_size,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_encode_and_decode_bucket_list_key() {
        // given
        let key = BucketListKey;

        // when
        let encoded = key.encode();
        let decoded = BucketListKey::decode(&encoded).unwrap();

        // then
        assert_eq!(decoded, key);
    }

    #[test]
    fn should_encode_and_decode_series_dictionary_key() {
        // given
        let key = SeriesDictionaryKey {
            time_bucket: 12345,
            series_fingerprint: 67890,
            bucket_size: 2,
        };

        // when
        let encoded = key.encode();
        let decoded = SeriesDictionaryKey::decode(&encoded).unwrap();

        // then
        assert_eq!(decoded, key);
    }

    #[test]
    fn should_encode_and_decode_forward_index_key() {
        // given
        let key = ForwardIndexKey {
            time_bucket: 12345,
            series_id: 42,
            bucket_size: 3,
        };

        // when
        let encoded = key.encode();
        let decoded = ForwardIndexKey::decode(&encoded).unwrap();

        // then
        assert_eq!(decoded, key);
    }

    #[test]
    fn should_encode_and_decode_inverted_index_key() {
        // given
        let key = InvertedIndexKey {
            time_bucket: 12345,
            attribute: "host".to_string(),
            value: "server1".to_string(),
            bucket_size: 1,
        };

        // when
        let encoded = key.encode();
        let decoded = InvertedIndexKey::decode(&encoded).unwrap();

        // then
        assert_eq!(decoded, key);
    }

    #[test]
    fn should_encode_and_decode_time_series_key() {
        // given
        let key = TimeSeriesKey {
            time_bucket: 12345,
            series_id: 99,
            bucket_size: 4,
        };

        // when
        let encoded = key.encode();
        let decoded = TimeSeriesKey::decode(&encoded).unwrap();

        // then
        assert_eq!(decoded, key);
    }

    #[test]
    fn should_create_attribute_range_that_matches_same_attribute_keys() {
        // given
        let bucket = crate::model::TimeBucket {
            start: 12345,
            size: 1,
        };
        let key1 = InvertedIndexKey {
            time_bucket: 12345,
            attribute: "host".to_string(),
            value: "server1".to_string(),
            bucket_size: 1,
        };
        let key2 = InvertedIndexKey {
            time_bucket: 12345,
            attribute: "host".to_string(),
            value: "server2".to_string(),
            bucket_size: 1,
        };
        let key3 = InvertedIndexKey {
            time_bucket: 12345,
            attribute: "env".to_string(),
            value: "prod".to_string(),
            bucket_size: 1,
        };

        // when
        let range = InvertedIndexKey::attribute_range(&bucket, "host");

        // then
        assert!(range.contains(&key1.encode()));
        assert!(range.contains(&key2.encode()));
        assert!(!range.contains(&key3.encode()));
    }

    #[test]
    fn should_not_match_shorter_attribute_with_value_that_looks_like_suffix() {
        // given - searching for "hostname" should NOT match a key with
        // attribute "host" and value "name" even though "host" + "name" = "hostname"
        // The tuple-style delimiter encoding should prevent this collision.
        let bucket = crate::model::TimeBucket {
            start: 12345,
            size: 1,
        };
        let host_name_key = InvertedIndexKey {
            time_bucket: 12345,
            attribute: "host".to_string(),
            value: "name".to_string(),
            bucket_size: 1,
        };

        // when - search for "hostname"
        let range = InvertedIndexKey::attribute_range(&bucket, "hostname");

        // then - should NOT match the "host":"name" key
        assert!(
            !range.contains(&host_name_key.encode()),
            "attribute_range for 'hostname' should not match key with attribute='host' value='name'. \
             The delimiter-based encoding should differentiate them."
        );
    }

    #[test]
    fn should_not_match_when_value_bytes_could_mimic_attribute_continuation() {
        // given - test a more contrived case where naive concatenation
        // might produce a collision
        let bucket = crate::model::TimeBucket {
            start: 12345,
            size: 1,
        };

        // Key with short attribute and value that concatenates to a different attribute
        let short_attr_key = InvertedIndexKey {
            time_bucket: 12345,
            attribute: "ab".to_string(),
            value: "cdef".to_string(),
            bucket_size: 1,
        };

        // Search for "abcdef"
        // If encoding was naive concatenation, "ab" + "cdef" might look like "abcdef"
        // But with tuple-style encoding, each element is delimited separately

        // when
        let range = InvertedIndexKey::attribute_range(&bucket, "abcdef");

        // then
        assert!(
            !range.contains(&short_attr_key.encode()),
            "attribute_range for 'abcdef' should not match key with attribute='ab' value='cdef'"
        );
    }
}
