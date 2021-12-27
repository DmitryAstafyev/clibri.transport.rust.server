use crate::{errors::Error, options::Ports as PortsSource};
use clibri::env::logs;
use log::info;
use std::{collections::HashMap, time::Instant};
use tokio_util::sync::CancellationToken;

const VALID_KEEP_OPEN_DURATION_MS: u128 = 3 * 1000;

pub struct Ports {
    source: Option<PortsSource>,
    per_port: Option<u32>,
    reserved: HashMap<u16, (u32, CancellationToken, Instant)>,
    used: HashMap<u16, u32>,
}

impl Ports {
    pub fn new(per_port: Option<u32>, source: Option<PortsSource>) -> Self {
        Self {
            source,
            per_port,
            reserved: HashMap::new(),
            used: HashMap::new(),
        }
    }

    pub fn add(&mut self, port: u16, cancel: tokio_util::sync::CancellationToken) {
        self.reserved.insert(port, (0, cancel, Instant::now()));
    }

    pub fn reserve(&mut self) -> Result<Option<u16>, Error> {
        if let Some(per_port) = self.per_port.as_ref() {
            let port = self
                .reserved
                .iter()
                .find_map(|(port, (count, _cancel, _instant))| {
                    if count < per_port {
                        Some(port.to_owned())
                    } else {
                        None
                    }
                });
            if let Some(port) = port {
                if let Some((count, _cancel, _instant)) = self.reserved.get_mut(&port) {
                    *count += 1;
                }
            }
            Ok(port)
        } else {
            Err(Error::NoOptions(String::from(
                "option connections_per_port is required",
            )))
        }
    }

    pub fn delegate(&mut self) -> Result<Option<u16>, Error> {
        let port = self.get_port()?;
        if port.is_some() {
            Ok(port)
        } else {
            self.drop_reserved();
            self.get_port()
        }
    }

    pub fn confirm(&mut self, port: u16) {
        *self.used.entry(port).or_insert(0) += 1;
    }

    pub fn free(&mut self, port: u16) {
        if let Some((count, cancel, _instant)) = self.reserved.get_mut(&port) {
            *count -= 1;
            if count == &0 {
                cancel.cancel();
                self.reserved.remove(&port);
            }
        }
        if let Some(count) = self.used.get_mut(&port) {
            *count -= 1;
            if count == &0 {
                self.used.remove(&port);
            }
        }
    }

    fn get_port(&mut self) -> Result<Option<u16>, Error> {
        if let Some(ref mut source) = self.source {
            match source {
                PortsSource::List(ports) => {
                    info!(
                        target: logs::targets::SERVER,
                        "looking for port from a list"
                    );
                    let mut free: Option<u16> = None;
                    for port in ports.iter() {
                        if !self.reserved.contains_key(port) {
                            free = Some(port.to_owned());
                            break;
                        }
                    }
                    Ok(free)
                }
                PortsSource::Range(range) => {
                    info!(
                        target: logs::targets::SERVER,
                        "looking for port from a range"
                    );
                    let mut free: Option<u16> = None;
                    for port in range {
                        if !self.reserved.contains_key(&port) {
                            free = Some(port);
                            break;
                        }
                    }
                    Ok(free)
                }
            }
        } else {
            Err(Error::NoOptions(String::from("option ports is required")))
        }
    }

    /// This method will drop all reserved ports to amount of real connections
    fn drop_reserved(&mut self) {
        let mut to_free: Vec<u16> = vec![];
        for (port, (reserved_connections, _cancel, instant)) in self.reserved.iter_mut() {
            if let Some(real_connections) = self.used.get(port) {
                *reserved_connections = *real_connections;
            } else if instant.elapsed().as_millis() >= VALID_KEEP_OPEN_DURATION_MS {
                to_free.push(*port);
            }
        }
        for port in to_free.iter() {
            self.free(*port);
        }
    }
}
