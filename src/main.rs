use clap::{Parser, ValueEnum};
use memcache::MemcacheError;
use rand::distributions::{Alphanumeric, DistString};
use rand::Rng;
use std::error::Error;
use std::vec;
use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use tokio::time::timeout;

use std::{collections::HashMap, sync::Arc};

const NUM_ENTRIES: usize = 10000;
const BUFFER_SIZE: usize = 1500;

#[derive(ValueEnum, Copy, Clone, Debug, PartialEq, Eq)]
enum Protocol {
    Udp,
    Tcp,
}

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[arg(short, long, default_value = "127.0.0.1")]
    server_address: String,

    #[arg(short, long, default_value = "11211")]
    port: String,

    /// key size to generate random memcached key
    #[arg(short, long, default_value = "12")]
    key_size: usize,

    /// value size to generate random memcached value
    #[arg(short, long, default_value = "12")]
    value_size: usize,

    /// verify the value after get command
    #[arg(short = 'd', long, default_value = "false")]
    validate: bool,

    /// number of test entries to generate
    #[arg(short, long, default_value = "100000")]
    nums: usize,

    // number of threads to run
    #[arg(short, long, default_value = "4")]
    threads: usize,

    /// udp or tcp protocol for memcached
    #[arg(short = 'l', long, default_value_t = Protocol::Udp , value_enum)]
    protocol: Protocol,
}

fn generate_random_str(len: usize) -> String {
    Alphanumeric.sample_string(&mut rand::thread_rng(), len)
}

fn generate_memcached_test_dict(
    key_size: usize,
    value_size: usize,
    nums: usize,
) -> HashMap<String, String> {
    // random generate dict for memcached test
    (0..nums)
        .map(|_| {
            (
                generate_random_str(key_size),
                generate_random_str(value_size),
            )
        })
        .collect()
}

fn set_memcached_value(
    server: &memcache::Client,
    test_dict: Arc<HashMap<String, String>>,
) -> std::result::Result<(), MemcacheError> {
    server.flush()?;

    // set a string value:
    for (key, value) in test_dict.iter() {
        server.set(key.as_str(), value, 0)?;
    }

    Ok(())
}

fn exmaple_method(server: &memcache::Client) -> std::result::Result<(), MemcacheError> {
    // flush the database:
    server.flush()?;

    // set a string value:
    server.set("foo", "bar", 0)?;

    // retrieve from memcached:
    let value: Option<String> = server.get("foo")?;
    assert_eq!(value, Some(String::from("bar")));
    assert_eq!(value.unwrap(), "bar");

    // prepend, append:
    server.prepend("foo", "foo")?;
    server.append("foo", "baz")?;
    let value: String = server.get("foo")?.unwrap();
    assert_eq!(value, "foobarbaz");

    // delete value:
    server.delete("foo").unwrap();

    // using counter:
    server.set("counter", 40, 0).unwrap();
    server.increment("counter", 2).unwrap();
    let answer: i32 = server.get("counter")?.unwrap();
    assert_eq!(answer, 42);

    println!("memcached server works!");
    Ok(())
}

async fn wrap_get_command(key: String, seq: u16) -> Vec<u8> {
    let mut bytes: Vec<u8> = vec![0, 0, 0, 1, 0, 0];
    let mut command = format!("get {}\r\n", key).into_bytes();
    let mut seq_bytes = seq.to_be_bytes().to_vec();
    seq_bytes.append(&mut bytes);
    seq_bytes.append(&mut command);
    // println!("bytes: {:?}", seq_bytes);
    seq_bytes
}

struct TaskData {
    buf: Vec<u8>,
    addr: String,
    key: String,
    test_dict: Arc<HashMap<String, String>>,
    validate: bool,
    key_size: usize,
    value_size: usize,
}

async fn socket_task(socket: Arc<UdpSocket>, mut rx: mpsc::Receiver<TaskData>) {
    while let Some(TaskData {
        buf,
        addr,
        key,
        test_dict,
        validate,
        key_size,
        value_size,
    }) = rx.recv().await
    {
        // Send
        let _ = socket.send_to(&buf[..], &addr).await;

        // Then receive
        let mut buf = [0; BUFFER_SIZE];
        let my_duration = tokio::time::Duration::from_millis(500);

        // timeout(my_duration, socket.recv_from(&mut buf)).await
        if let Ok(Ok((amt, _))) = timeout(my_duration, socket.recv_from(&mut buf)).await {
            if validate {
                if let Some(value) = test_dict.get(&key) {
                    let received = String::from_utf8_lossy(&buf[..amt])
                        .split("VALUE ")
                        .nth(1)
                        .unwrap_or_default()[6 + key_size + 1..6 + key_size + value_size + 1]
                        .to_string();

                    if received != *value.to_string() {
                        println!(
                            "response not match key {} buf: {} , value: {}",
                            key, received, value
                        );
                    }
                }
            }
        }
    }
}

// TODO add mutiple thread support
async fn get_command_benchmark(
    test_dict: Arc<HashMap<String, String>>,
    nums: usize,
) -> Result<(), Box<dyn Error>> {
    let args = Cli::parse();
    let keys: Vec<&String> = test_dict.keys().collect();

    // assign client address
    let addr = format!("{}:{}", args.server_address, args.port);
    let socket = UdpSocket::bind("0.0.0.0:0").await?;
    let socket = Arc::new(socket);

    let start = std::time::Instant::now();
    let dict_len = keys.len();

    let mut seq: u16 = 0;

    // Create the channel
    let (tx, rx) = mpsc::channel(100000);
    let socket_clone = Arc::clone(&socket);
    let socket_task = tokio::spawn(socket_task(socket_clone, rx));

    for _ in 0..nums {
        let rng = rand::thread_rng().gen_range(0..dict_len - 1);
        let key = keys[rng].clone();
        // let addr_clone = Arc::clone(&addr);
        let packet = wrap_get_command(key.clone(), seq).await;
        seq = seq.wrapping_add(1);

        let send_result = tx
            .send(TaskData {
                buf: packet,
                addr: addr.clone(),
                key,
                test_dict: test_dict.clone(),
                validate: args.validate,
                key_size: args.key_size,
                value_size: args.value_size,
            })
            .await;
        if send_result.is_err() {
            // The receiver was dropped, break the loop
            break;
        }
    }

    // Close the channel
    drop(tx);

    // Wait for the socket task to finish
    socket_task.await?;

    let duration = start.elapsed();
    println!("Time elapsed in get_command_benchmark() is: {:?}", duration);

    Ok(())
}

fn get_server(
    addr: &String,
    port: &String,
    protocol: &Protocol,
) -> Result<memcache::Client, MemcacheError> {
    match protocol {
        Protocol::Udp => memcache::connect(format!("memcache+udp://{}:{}?timeout=10", addr, port)),
        Protocol::Tcp => memcache::connect(format!("memcache://{}:{}?timeout=10", addr, port)),
    }
}

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn Error>> {
    let args = Cli::parse();

    let server = get_server(&args.server_address, &args.port, &args.protocol)?;
    exmaple_method(&server)?;

    let test_dict = generate_memcached_test_dict(args.key_size, args.value_size, NUM_ENTRIES);

    let test_dict = Arc::new(test_dict);

    // assign test_dict to server
    set_memcached_value(&server, test_dict.clone())?;

    let mut handles = vec![];

    for _ in 0..args.threads {
        let test_dict = Arc::clone(&test_dict);
        let handle = tokio::spawn(async move {
            match get_command_benchmark(test_dict, args.nums).await {
                Ok(_) => (),
                Err(e) => eprintln!("Task failed with error: {:?}", e),
            }
        });
        handles.push(handle);
    }

    // wait for all tasks to complete
    for handle in handles {
        handle.await?;
    }

    // stats
    let stats = server.stats()?;
    println!("stats: {:?}", stats);

    Ok(())
}
