use clap::Parser;
use futures::StreamExt;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::future::Future;
use std::net::{Ipv6Addr, SocketAddr, SocketAddrV6};
use std::num::{NonZeroU16, NonZeroUsize};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpSocket, TcpStream};
use tokio::sync::mpsc::{unbounded_channel, Sender, UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot::Receiver;
use tokio::sync::Mutex;
use tokio::task::{JoinError, JoinSet};
use tokio::{join, select};
use tokio_util::bytes::BufMut;
use tokio_util::codec::{FramedRead, LinesCodec, LinesCodecError};
use tracing::{error, info, trace, warn};

#[derive(Debug, Parser)]
#[command(version, about, long_about = None)]
/// Host a simple hole punching server for the polyhedron game (lobby).
struct Cli {
    #[clap(short, long)]
    /// The port the server listens to.
    port: NonZeroU16,
    #[clap(short, long)]
    /// The maximum number of connections before all are dropped.
    limit: NonZeroUsize, /*
                         #[clap(short, long, default_value = "0.25")]
                         /// The time interval between keep alive packets.  In seconds.
                         interval: f32,
                         #[clap(short, long, default_value = "1.0")]
                         /// The timeout since the last packet received from a player before it is considered to be dropped.
                         timeout: f32,*/
}

#[derive(Debug)]
enum RoomMessage {
    Join { room: String, client: NewClient },
    Broadcast { source: SocketAddr, line: String },
    Leave { address: SocketAddr },
}

#[derive(Debug)]
struct JoinRoom {
    room: String,
    client: NewClient,
}

#[derive(Debug)]
struct NewClient {
    address: SocketAddr,
    read: FramedRead<OwnedReadHalf, LinesCodec>,
    write: OwnedWriteHalf,
}

#[derive(Debug)]
struct Client {
    room: String,
    line_sender: UnboundedSender<Arc<str>>,
}

#[tokio::main]
async fn main() {
    tracing::subscriber::set_global_default(tracing_subscriber::FmtSubscriber::new()).unwrap();

    let cli = Cli::parse();

    let (rooms_sender, room_receiver) = unbounded_channel();
    tokio::spawn(sync_rooms(rooms_sender.clone(), room_receiver));

    loop {
        info!("Starting server");
        let mut socket = TcpListener::bind(SocketAddrV6::new(Ipv6Addr::UNSPECIFIED, cli.port.get(), 0, 0))
            .await
            .unwrap();
        let mut connections_count = Arc::new(AtomicUsize::new(0));
        while connections_count.load(Ordering::Relaxed) < cli.limit.get() {
            let (stream, address) = socket.accept().await.unwrap();
            tokio::spawn(new_client(stream, address, rooms_sender.clone()));
            connections_count.fetch_add(1, Ordering::Relaxed);
        }
    }
}

async fn sync_rooms(rooms_sender: UnboundedSender<RoomMessage>, mut room_receiver: UnboundedReceiver<RoomMessage>) {
    let mut rooms = HashMap::new();
    let mut clients: HashMap<SocketAddr, Client> = HashMap::new();
    while let Some(message) = room_receiver.recv().await {
        match message {
            RoomMessage::Join {
                room,
                client: NewClient { address, read, write },
            } => {
                let (line_sender, line_receiver) = unbounded_channel();
                let old = rooms.entry(room.clone()).or_insert_with(Vec::new);
                let line = Arc::<str>::from(format!("{},{} J\n", address.ip(), address.port()));
                for address in &*old {
                    clients[&address].line_sender.send(line.clone()).unwrap();
                    line_sender.send(Arc::<str>::from(format!("{},{} J\n", address.ip(), address.port()))).unwrap()
                }
                clients.insert(address, Client { room, line_sender });
                old.push(address);
                tokio::spawn(client(rooms_sender.clone(), address, read, write, line_receiver));
            }
            RoomMessage::Broadcast { source, line } => {
                let line = Arc::<str>::from(format!("{},{} {}\n", source.ip(), source.port(), line));
                for destination in &rooms[&clients[&source].room] {
                    clients[destination].line_sender.send(line.clone()).unwrap();
                }
            }
            RoomMessage::Leave { address } => {
                let client = clients.remove(&address).unwrap();
                let Entry::Occupied(mut occ) = rooms.entry(client.room.clone()) else {
                    panic!("Missing room {}", client.room)
                };
                let i = occ.get().iter().position(|r| r == &address).unwrap();
                occ.get_mut().swap_remove(i);
                if occ.get().is_empty() {
                    occ.remove();
                } else {
                    let line = Arc::<str>::from(format!("{},{} L\n", address.ip(), address.port()));
                    for address in occ.get() {
                        clients[&address].line_sender.send(line.clone()).unwrap()
                    }
                }
            }
        }
    }
}

async fn client(
    rooms_sender: UnboundedSender<RoomMessage>,
    address: SocketAddr,
    mut read: FramedRead<OwnedReadHalf, LinesCodec>,
    mut write: OwnedWriteHalf,
    mut line_receiver: UnboundedReceiver<Arc<str>>,
) {
    loop {
        select! {
            biased;
            option = line_receiver.recv() => {
                let Some(line) = option else {
                    error!("Client receiver closed");
                    break
                };
                match write.write(line.as_bytes()).await {
                    Ok(0) => {
                        warn!("Failed to write to remote socket {address}");
                        break
                    }
                    Ok(amount) => {
                        assert_eq!(amount, line.len());
                        trace!("Wrote {amount} bytes from {address}");
                        if let Err(error) = write.flush().await {
                            error!(%error, "Failed to flush to remote socket {address}");
                            break
                        }
                    }
                    Err(error) => {
                        error!(%error, "Failed to write to remote socket {address}");
                        break
                    }
                }
                write.flush().await.unwrap();
            }
            result = read.next() => {
                match result {
                    None => break,
                    Some(Err(LinesCodecError::Io(error))) => {
                        error!(%error, "IO error receiving from remote socket {address}");
                        break
                    },
                    Some(Err(LinesCodecError::MaxLineLengthExceeded))   =>  unreachable!(),
                    Some(Ok(line)) => {
                        rooms_sender.send(RoomMessage::Broadcast { source: address, line }).unwrap()
                    }
                }
            }
        }
    }
    rooms_sender.send(RoomMessage::Leave { address }).unwrap();
    read.into_inner().reunite(write).unwrap().shutdown().await.unwrap();
}
/*
async fn to_client(
    room_sender: UnboundedSender<RoomMessage>,
    address: SocketAddr,
    mut write: OwnedWriteHalf,
    mut client_receiver: UnboundedReceiver<ClientMessage>,
) {
    while let Some(message) = client_receiver.recv().await {
        match message {
            ClientMessage::Line(line) => {
                match write.write(line.as_bytes()).await {
                    Ok(0) => {
                        warn!("Failed to write to remote socket {address}");
                        break
                    }
                    Ok(amount) => {
                        assert_eq!(amount, line.len());
                        if let Err(error) = write.flush().await {
                            error!(%error, "Failed to flush to remote socket {address}");
                            break
                        }
                    }
                    Err(error) => {
                        error!(%error, "Failed to write to remote socket {address}");
                        break
                    }
                }
                write.flush().await.unwrap();
            }
            ClientMessage::LeavingRoom => {
                write.shutdown().await.unwrap();
                return
            }
        }
    }
    room_sender.send(RoomMessage::Leave { address }).unwrap();
    write.shutdown().await.unwrap();
}
*/
async fn new_client(mut stream: TcpStream, remote: SocketAddr, rooms: UnboundedSender<RoomMessage>) {
    let (read, write) = stream.into_split();
    let mut lines = FramedRead::new(read, LinesCodec::new());

    match lines.next().await {
        Some(Err(LinesCodecError::MaxLineLengthExceeded)) => unreachable!(),
        Some(Err(LinesCodecError::Io(error))) => {
            error!(%error, "IO error getting room");
            return;
        }
        None => return,
        Some(Ok(room)) => {
            rooms
                .send(RoomMessage::Join {
                    room,
                    client: NewClient {
                        address: remote,
                        read: lines,
                        write,
                    },
                })
                .unwrap();
        }
    }
}

/*
async fn run_room(
    mut lines: FramedRead<OwnedReadHalf, LinesCodec>,
    mut clients: &mut HashMap<SocketAddr,
    Arc<Mutex<OwnedWriteHalf>>>
) {
    loop {
        match lines.next().await {
            Some(Err(LinesCodecError::MaxLineLengthExceeded)) => unreachable!(),
            Some(Err(LinesCodecError::Io(error))) => {
                error!(%error, "IO error getting line");
                break
            }
            None => break,
            Some(Ok(mut line)) => {
                line.push('\n');
                let mut set = JoinSet::new();
                for (address, write) in clients {
                    let (address, mut write) = (address.clone(), write.clone());
                    let bytes: Box<_> = line.as_bytes().into();
                    set.spawn(async move {
                        let mut write = write.lock().await;
                        match write.write(&bytes).await {
                            Ok(0) => {
                                error!("Connection to {} closed", address);
                                Some(address)
                            }
                            Err(error) => {
                                error!(%error, "IO error in writing");
                                Some(address)
                            }
                            Ok(_) => {
                                write.flush().await.unwrap();
                                None
                            }
                        }
                    });
                }
                while let Some(result) = set.join_next().await {
                    match result {
                        Ok(None) => {}
                        Err(error) => {
                            error!(%error, "Error joining forwarding");
                        }
                        Ok(Some(address)) => {
                            clients.remove(&address);
                        }
                    }
                }
            }
        }
    }
}*/
/*
fn write_join_packet<const N: usize>(packet: &mut ArrayVec<u8, N>, address: &SocketAddr) {
    match address {
        SocketAddr::V4(address) => {
            packet.push(4);
            packet.extend(address.ip().octets());
            packet.extend(address.port().to_be_bytes());
        }
        SocketAddr::V6(address) => {
            packet.push(6);
            packet.extend(address.ip().octets());
            packet.extend(address.port().to_be_bytes());
        }
    }
}
fn write_leave_packet<const N: usize>(packet: &mut ArrayVec<u8, N>, address: &SocketAddr) {
    match address {
        SocketAddr::V4(address) => {
            packet.push(-4 as _);
            packet.extend(address.ip().octets());
            packet.extend(address.port().to_be_bytes());
        }
        SocketAddr::V6(address) => {
            packet.push(-6 as _);
            packet.extend(address.ip().octets());
            packet.extend(address.port().to_be_bytes());
        }
    }
}
*/
