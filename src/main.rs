use arrayvec::ArrayVec;
use clap::Parser;
use futures::FutureExt;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::net::{Ipv6Addr, SocketAddr};
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::UdpSocket;
use tokio::select;
use tokio::sync::mpsc;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::task::JoinSet;
use tokio::time::{interval, sleep, Instant};

#[derive(Debug, Parser)]
#[command(version, about, long_about = None)]
/// Host a simple hole punching server for the polyhedron game (lobby).
struct Cli {
    #[clap(short, long)]
    /// The port the server listens to.
    port: u16,
    #[clap(short, long)]
    /// The maximum number of connections before all are dropped.
    limit: NonZeroUsize,
    #[clap(short, long, default_value = "0.25")]
    /// The time interval between keep alive packets.  In seconds.
    interval: f32,
    #[clap(short, long, default_value = "1.0")]
    /// The timeout since the last packet received from a player before it is considered to be dropped.
    timeout: f32,
}

#[derive(Debug)]
struct ClientMessage {
    /// Joining or leaving.
    joining: bool,
    address: SocketAddr,
}

#[derive(Debug)]
struct Connection {
    message: UnboundedSender<ClientMessage>,
    refresh: UnboundedSender<()>,
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    if cli.interval <= 0.0 {
        eprintln!("Keep alive interval must be greater than 0s");
        std::process::exit(1);
    }
    if cli.timeout <= 0.0 {
        eprintln!("Timeout must be greater than 0s");
        std::process::exit(2);
    }

    loop {
        let socket = Arc::new(UdpSocket::bind(std::net::SocketAddrV6::new(Ipv6Addr::LOCALHOST, cli.port, 0, 0)).await.unwrap());
        let mut connections = HashMap::<SocketAddr, Connection>::new();
        let mut rooms_map = HashMap::<_, UnboundedSender<_>>::new();
        let mut rooms_tasks = JoinSet::new();
        let mut timeouts = JoinSet::new();
        let mut buf = Vec::with_capacity(1024);
        let mut interval = interval(Duration::from_secs_f32(cli.interval));
        while connections.len() < cli.limit.get() {
            buf.clear();
            select! {
                biased;
                free_room = rooms_tasks.join_next(), if !rooms_tasks.is_empty() => {
                    match free_room.unwrap() {
                        Ok(free_room) => {
                            rooms_map.remove(&free_room);
                        }
                        Err(err) => {
                            eprintln!("couldn't free room: {}", err);
                            break
                        }
                    }
                }
                address = timeouts.join_next(), if !timeouts.is_empty() => {
                    match address.unwrap() {
                        Ok(address) => {
                            let Some(removed) = connections.remove(&address) else {
                                eprintln!("Attempted to remove missing connection");
                                break
                            };
                            removed.message.send(ClientMessage { joining: false, address }).unwrap();
                            eprintln!("Dropping connection to {}", address);
                        }
                        Err(err) => {
                            eprintln!("error in getting timeout: {}", err);
                            break
                        }
                    }
                }
                _ = interval.tick() => {
                    for connection in connections.keys().cloned() {
                        let socket = socket.clone();
                        tokio::spawn(async move { socket.send_to(&[], connection).await.unwrap() });
                    }
                }
                result = socket.recv_buf_from(&mut buf) => {
                    match result {
                        Ok((read, address)) => {
                            println!("Received packet from {address}");
                            assert_eq!(read, buf.len());
                            match buf.first().map(|&first| first as i8) {
                                //  Connect to a room
                                Some(1)   =>  {
                                    if buf.len() <= 2 {
                                        eprintln!("Join invalid format: too short (len = {})", buf.len());
                                    }
                                    if buf[0] != 1 {
                                        eprintln!("Join invalid format: buf[0]={} != 1", buf[0]);
                                        break
                                    }
                                    let raw_room = &buf[2..];
                                    if buf[1] as usize != raw_room.len() {
                                        eprintln!("Join invalid format: buf[1] != raw_room.len()={}", raw_room.len());
                                        break
                                    }
                                    let Ok(room) = std::str::from_utf8(raw_room) else {
                                        eprintln!("Join invalid format: room name not valid UTF-8");
                                        break
                                    };
                                    let room = room.trim();
                                    if room.is_empty() {
                                        eprintln!("Join invalid format: room name is empty string");
                                    }
                                    let Entry::Vacant(vac) = connections.entry(address.clone()) else { break };
                                    let room = room.to_string();
                                    let messsage_sender = match rooms_map.entry(room.clone()) {
                                        Entry::Occupied(occ)    =>  {
                                            occ.get().send(ClientMessage { joining: true, address }).unwrap();
                                            occ.get().clone()
                                        }
                                        Entry::Vacant(vac) => {
                                            let (sender, receiver) = mpsc::unbounded_channel();
                                            let room = room.clone();
                                            rooms_tasks.spawn(run_room(socket.clone(), address, receiver).then(|()| async move { room }));
                                            vac.insert(sender).clone()
                                        }
                                    };
                                    let (refresh_sender, refresh_receiver) = mpsc::unbounded_channel();
                                    timeouts.spawn(timeout(Duration::from_secs_f32(cli.timeout), refresh_receiver).then(move |()| async move { address }));
                                    vac.insert(Connection { message: messsage_sender, refresh: refresh_sender });
                                }
                                //  Disconnect from a room
                                Some(-1)  =>  {
                                    if buf.len() != 0 {
                                        eprintln!("Disconnect invalid format: too long (len = {})", buf.len());
                                        break
                                    }
                                    let Some(sender) = connections.get(&address) else {
                                        eprintln!("Disconnect: unknown sender {}", address);
                                        break
                                    };
                                    sender.message.send(ClientMessage { joining: false, address }).unwrap();
                                }
                                //  A keep alive packet
                                None    =>  {
                                    let Some(connection) = connections.get(&address) else {
                                        eprintln!("Received keep alive packet from unknown sender {}", address);
                                        continue
                                    };
                                    connection.refresh.send(()).unwrap();
                                }
                                Some(other)   =>  {
                                    eprintln!("Unrecognized packet type: {} with length {}", other, read);
                                }
                            }
                        }
                        Err(err) => {
                            eprintln!("Error reading from socket; err={}", err);
                            break
                        }
                    }
                }
            }
        }
        eprintln!("Maximum number of connections reached ({} >= {})", connections.len(), cli.limit);
    }
}

async fn run_room(socket: Arc<UdpSocket>, initial: SocketAddr, mut messages: UnboundedReceiver<ClientMessage>) {
    let mut addresses = vec![initial];
    while !addresses.is_empty() || !messages.is_empty() {
        match messages.recv().await {
            Some(ClientMessage { joining, address: new_address }) => {
                let mut joined_player_packet = ArrayVec::<u8, {1 + size_of::<Ipv6Addr>() + size_of::<u16>()}>::new();
                if joining {
                    write_join_packet(&mut joined_player_packet, &new_address);
                    for old_address in addresses.iter().cloned() {
                        let packet = joined_player_packet.clone();
                        let (s1, s2) = (socket.clone(), socket.clone());
                        tokio::spawn(async move { s1.send_to(&packet, old_address).await.unwrap() });
                        let mut old_player_packet = ArrayVec::<u8, {1 + size_of::<Ipv6Addr>() + size_of::<u16>()}>::new();
                        write_join_packet(&mut old_player_packet, &old_address);
                        tokio::spawn(async move { s2.send_to(&old_player_packet, new_address).await.unwrap() });
                    }
                    addresses.push(new_address);
                } else {
                    write_leave_packet(&mut joined_player_packet, &new_address);
                    addresses.swap_remove(addresses.iter().position(|old_address| new_address == *old_address).unwrap());
                    for destination in addresses.iter().cloned() {
                        let socket = socket.clone();
                        let packet = joined_player_packet.clone();
                        tokio::spawn(async move { socket.send_to(&packet, destination).await.unwrap() });
                    }
                }
            }
            None    =>  unreachable!(),
        }
    }
}

async fn timeout(period: Duration, mut refresh: UnboundedReceiver<()>) {
    let sleep = sleep(period);
    tokio::pin!(sleep);
    loop {
        select! {
            biased;
            option = refresh.recv() => {
                if option.is_some() {
                    sleep.as_mut().reset(Instant::now() + period);
                } else {
                    return
                }
            }
            _ = &mut sleep => {
                return
            }
        }
    }
}

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

