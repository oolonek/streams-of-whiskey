use std::io::Read;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use reqwest::StatusCode;
use reqwest::Url;
use reqwest::blocking::Client;
use reqwest::header::{CONTENT_RANGE, RANGE};

use crate::rate::RateGate;
use crate::send_with_gate;

const MAX_REQUEST_BYTES: usize = 2 * 1024 * 1024;
const MAX_FILE_BYTES: u64 = 16 * 1024 * 1024;

#[derive(Clone)]
pub struct RangeSource {
    pub size: u64,
    client: Client,
    gate: Arc<RateGate>,
    url: Url,
    consumed: Arc<AtomicU64>,
    max_bytes: Option<u64>,
    #[cfg(test)]
    local: Option<Bytes>,
}

pub struct FileSample {
    pub found: Option<(u64, String)>,
    pub sampled_rows: usize,
    pub wide: bool,
}

impl RangeSource {
    pub fn new(client: Client, gate: Arc<RateGate>, url: Url, size: u64) -> Self {
        Self::with_limit(client, gate, url, size, Some(MAX_FILE_BYTES))
    }

    /// Explicit full-file inspection has no aggregate transfer cap; every HTTP range
    /// remains bounded to MAX_REQUEST_BYTES and validates Content-Range.
    pub fn new_unbounded(client: Client, gate: Arc<RateGate>, url: Url, size: u64) -> Self {
        Self::with_limit(client, gate, url, size, None)
    }

    fn with_limit(
        client: Client,
        gate: Arc<RateGate>,
        url: Url,
        size: u64,
        max_bytes: Option<u64>,
    ) -> Self {
        Self {
            size,
            client,
            gate,
            url,
            consumed: Arc::new(AtomicU64::new(0)),
            max_bytes,
            #[cfg(test)]
            local: None,
        }
    }

    #[cfg(test)]
    pub(crate) fn from_bytes(bytes: &[u8]) -> Self {
        let data = Bytes::copy_from_slice(bytes);
        Self {
            size: data.len() as u64,
            client: Client::new(),
            gate: Arc::new(RateGate::new()),
            url: Url::parse("http://localhost/").unwrap(),
            consumed: Arc::new(AtomicU64::new(0)),
            max_bytes: Some(MAX_FILE_BYTES),
            local: Some(data),
        }
    }

    pub fn read(&self, offset: u64, len: usize) -> Result<Bytes, String> {
        if len == 0 {
            return Ok(Bytes::new());
        }
        if len > MAX_REQUEST_BYTES {
            return Err("requested range exceeds the 2 MiB request limit".into());
        }
        let end = offset
            .checked_add(len as u64)
            .ok_or("range overflows file offset")?;
        if end > self.size {
            return Err("requested range exceeds file size".into());
        }
        self.consumed
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |used| {
                used.checked_add(len as u64)
                    .filter(|total| self.max_bytes.is_none_or(|max| *total <= max))
            })
            .map_err(|_| {
                if self.max_bytes.is_some() {
                    "file range-read budget exceeded (16 MiB)"
                } else {
                    "file range-read byte counter overflowed"
                }
            })?;

        #[cfg(test)]
        if let Some(local) = &self.local {
            return Ok(local.slice(offset as usize..end as usize));
        }

        let range = format!("bytes={offset}-{}", end - 1);
        let response = send_with_gate(
            &self.client,
            self.client.get(self.url.clone()).header(RANGE, range),
            &self.gate,
        )?;
        if response.status() != StatusCode::PARTIAL_CONTENT {
            if !response.status().is_success() {
                return Err(format!("resolver HTTP {}", response.status()));
            }
            return Err(format!(
                "resolver ignored Range (HTTP {}); refusing full download",
                response.status()
            ));
        }
        let expected = format!("bytes {offset}-{}/{}", end - 1, self.size);
        let actual = response
            .headers()
            .get(CONTENT_RANGE)
            .and_then(|value| value.to_str().ok())
            .ok_or("resolver omitted Content-Range")?;
        if actual != expected {
            return Err(format!(
                "resolver returned unexpected Content-Range: {actual}"
            ));
        }
        if response
            .content_length()
            .is_some_and(|size| size != len as u64)
        {
            return Err("resolver returned a range with incorrect length".into());
        }
        let mut data = Vec::with_capacity(len);
        response
            .take(len as u64 + 1)
            .read_to_end(&mut data)
            .map_err(|err| err.to_string())?;
        if data.len() != len {
            return Err("resolver returned a partial or oversized range".into());
        }
        Ok(Bytes::from(data))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounds_local_range_reads_without_downloads() {
        let source = RangeSource::from_bytes(b"header\nCCO\n");
        assert_eq!(source.read(7, 3).unwrap().as_ref(), b"CCO");
        assert!(source.read(0, MAX_REQUEST_BYTES + 1).is_err());
        assert!(source.read(11, 1).is_err());
    }

    #[test]
    fn refuses_resolver_that_ignores_range_without_reading_body() {
        use std::io::Write;
        use std::net::TcpListener;
        use std::time::Duration;

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = Url::parse(&format!("http://{}/file", listener.local_addr().unwrap())).unwrap();
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let mut request = [0; 2048];
            let size = stream.read(&mut request).unwrap();
            let request = String::from_utf8_lossy(&request[..size]);
            assert!(request.contains("range: bytes=2-4") || request.contains("Range: bytes=2-4"));
            stream
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Length: 1000000000\r\nConnection: close\r\n\r\n",
                )
                .unwrap();
        });
        let source = RangeSource::new(
            Client::builder()
                .no_proxy()
                .timeout(Duration::from_secs(5))
                .build()
                .unwrap(),
            Arc::new(RateGate::new()),
            url,
            1_000_000_000,
        );
        let error = source.read(2, 3).unwrap_err();
        server.join().unwrap();
        assert!(error.contains("refusing full download"), "{error}");
    }
}
