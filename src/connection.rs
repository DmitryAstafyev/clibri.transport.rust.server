use super::{
    channel::{Control, Error as ChannelError, Messages},
    errors::Error,
    server::MonitorEvent,
};
use clibri::{env::logs, server};
use futures::{SinkExt, StreamExt};
use log::{debug, error, info, warn};
use tokio::{
    join,
    net::TcpStream,
    select,
    sync::{
        mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
        oneshot,
    },
    task::spawn,
};
use tokio_tungstenite::{
    tungstenite::{
        error::{Error as TungsteniteError, ProtocolError},
        protocol::CloseFrame,
        protocol::Message,
    },
    WebSocketStream,
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

enum State {
    DisconnectByClient(Option<CloseFrame<'static>>),
    DisconnectByClientWithError(String),
    DisconnectByServer,
    Error(ChannelError),
}

pub struct Connection {
    uuid: Uuid,
}

impl Connection {
    pub fn new(uuid: Uuid) -> Self {
        Self { uuid }
    }

    pub async fn attach(
        &mut self,
        ws: WebSocketStream<TcpStream>,
        events: UnboundedSender<server::Events<Error>>,
        messages: UnboundedSender<Messages>,
        monitor: Option<UnboundedSender<(u16, MonitorEvent)>>,
        port: u16,
    ) -> Result<UnboundedSender<Control>, String> {
        let (tx_control, mut rx_control): (UnboundedSender<Control>, UnboundedReceiver<Control>) =
            unbounded_channel();
        let uuid = self.uuid;
        let incomes_task_events = events.clone();
        let send_event = move |event: server::Events<Error>| {
            if let Err(e) = incomes_task_events.send(event) {
                warn!(
                    target: logs::targets::SERVER,
                    "Cannot send event. Error: {}", e
                );
            }
        };
        if let Some(monitor) = monitor.as_ref() {
            monitor
                .send((port, MonitorEvent::Connected))
                .map_err(|_e| String::from("Fail send monitor event - connected"))?;
        }
        let send_message = move |msg: Messages| {
            match messages.send(msg) {
                Ok(_) => {}
                Err(e) => {
                    warn!(
                        target: logs::targets::SERVER,
                        "{}:: Fail to send data back to server. Error: {}", uuid, e
                    );
                    if let Err(e) = events.send(server::Events::ConnectionError(
                        Some(uuid),
                        Error::Channel(format!("{}", e)),
                    )) {
                        warn!(
                            target: logs::targets::SERVER,
                            "Cannot send event. Error: {}", e
                        );
                    }
                }
            };
        };
        spawn(async move {
            let mut shutdown_resolver: Option<oneshot::Sender<()>> = None;
            let (mut writer, mut reader) = ws.split();
            let stop_reading = CancellationToken::new();
            let stop_reading_emitter = stop_reading.clone();
            let stop_writing = CancellationToken::new();
            let stop_writing_emitter = stop_writing.clone();
            let ((writer, writer_state), (reader, mut reader_state)) = join!(
                async {
                    while let Some(msg) = select! {
                        msg = reader.next() => msg,
                        _ = stop_reading.cancelled() => None,
                    } {
                        let msg = match msg {
                            Ok(msg) => msg,
                            Err(err) => {
                                if let TungsteniteError::Protocol(ref err) = err {
                                    if err == &ProtocolError::ResetWithoutClosingHandshake {
                                        debug!(
                                            target: logs::targets::SERVER,
                                            "{}:: Client disconnected without closing handshake",
                                            uuid
                                        );
                                        stop_writing_emitter.cancel();
                                        return (
                                            reader,
                                            Some(State::DisconnectByClientWithError(
                                                err.to_string(),
                                            )),
                                        );
                                    }
                                }
                                warn!(
                                    target: logs::targets::SERVER,
                                    "{}:: Cannot get message. Error: {:?}", uuid, err
                                );
                                send_event(server::Events::ConnectionError(
                                    Some(uuid),
                                    Error::InvalidMessage(err.to_string()),
                                ));
                                stop_writing_emitter.cancel();
                                return (
                                    reader,
                                    Some(State::Error(ChannelError::ReadSocket(err.to_string()))),
                                );
                            }
                        };
                        match msg {
                            Message::Text(_) => {
                                warn!(
                                    target: logs::targets::SERVER,
                                    "{}:: has been gotten not binnary data", uuid
                                );
                                send_event(server::Events::ConnectionError(
                                    Some(uuid),
                                    Error::NonBinaryData,
                                ));
                                continue;
                            }
                            Message::Binary(buffer) => {
                                info!(
                                    target: logs::targets::SERVER,
                                    "{}:: binary data {:?}", uuid, buffer
                                );
                                send_message(Messages::Binary { uuid, buffer });
                            }
                            Message::Ping(_) | Message::Pong(_) => {
                                warn!(target: logs::targets::SERVER, "{}:: Ping / Pong", uuid);
                            }
                            Message::Close(close_frame) => {
                                stop_writing_emitter.cancel();
                                return (reader, Some(State::DisconnectByClient(close_frame)));
                            }
                        }
                    }
                    stop_writing_emitter.cancel();
                    (reader, None)
                },
                async {
                    while let Some(cmd) = select! {
                        cmd = rx_control.recv() => cmd,
                        _ = stop_writing.cancelled() => None,
                    } {
                        match cmd {
                            Control::Send(buffer) => {
                                if let Err(err) = writer.send(Message::from(buffer)).await {
                                    error!(
                                        target: logs::targets::SERVER,
                                        "{}:: Cannot send data to client. Error: {}", uuid, err
                                    );
                                    stop_reading_emitter.cancel();
                                    return (
                                        writer,
                                        Some(State::Error(ChannelError::WriteSocket(
                                            err.to_string(),
                                        ))),
                                    );
                                }
                            }
                            Control::Disconnect(tx_shutdown_resolver) => {
                                shutdown_resolver = Some(tx_shutdown_resolver);
                                stop_reading_emitter.cancel();
                                return (writer, Some(State::DisconnectByServer));
                            }
                        };
                    }
                    stop_reading_emitter.cancel();
                    (writer, None)
                }
            );

            debug!(
                target: logs::targets::SERVER,
                "{}:: exit from socket listening loop.", uuid
            );
            let state: Option<State> = if let Some(state) = reader_state.take() {
                Some(state)
            } else {
                writer_state
            };
            if let Some(state) = state {
                match state {
                    State::DisconnectByServer => {
                        send_message(Messages::Disconnect { uuid, code: None });
                    }
                    State::DisconnectByClient(frame) => {
                        send_message(Messages::Disconnect {
                            uuid,
                            code: if let Some(frame) = frame {
                                Some(frame.code)
                            } else {
                                None
                            },
                        });
                    }
                    State::DisconnectByClientWithError(e) => {
                        debug!(
                            target: logs::targets::SERVER,
                            "{}:: client error: {}", uuid, e
                        );
                        send_message(Messages::Disconnect { uuid, code: None });
                    }
                    State::Error(error) => {
                        send_message(Messages::Error { uuid, error });
                    }
                };
            }
            match writer.reunite(reader) {
                Ok(mut ws) => {
                    match ws.close(None).await {
                        Ok(()) => {}
                        Err(e) => match e {
                            TungsteniteError::AlreadyClosed
                            | TungsteniteError::ConnectionClosed => {
                                debug!(
                                    target: logs::targets::SERVER,
                                    "{}:: connection is already closed", uuid
                                );
                            }
                            _ => {
                                error!(
                                    target: logs::targets::SERVER,
                                    "{}:: fail to close connection", uuid
                                );
                                send_event(server::Events::ConnectionError(
                                    Some(uuid),
                                    Error::CloseConnection(format!(
                                        "{}:: fail to close connection",
                                        uuid
                                    )),
                                ));
                            }
                        },
                    };
                    drop(ws);
                }
                Err(err) => {
                    error!(
                        target: logs::targets::SERVER,
                        "{}:: fail to close connection (reunite err: {})", uuid, err
                    );
                    send_event(server::Events::ConnectionError(
                        Some(uuid),
                        Error::CloseConnection(format!(
                            "{}:: fail to close connection (reunite err: {})",
                            uuid, err
                        )),
                    ));
                }
            }
            if let Some(monitor) = monitor.as_ref() {
                if let Err(_err) = monitor.send((port, MonitorEvent::Disconnected)) {
                    error!(
                        target: logs::targets::SERVER,
                        "{}:: Fail send monitor event - disconnected", uuid
                    );
                }
            }
            if let Some(tx_shutdown_resolver) = shutdown_resolver.take() {
                if tx_shutdown_resolver.send(()).is_err() {
                    error!(
                        target: logs::targets::SERVER,
                        "{}:: Fail send disconnect confirmation", uuid
                    );
                }
            }
        });
        Ok(tx_control)
    }
}
