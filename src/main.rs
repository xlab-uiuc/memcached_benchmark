use clap::{Parser, ValueEnum};
use memcache::MemcacheError;
use rand::distributions::{Alphanumeric, DistString};
use rand::Rng;

use std::{collections::HashMap, sync::Arc};

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
    #[arg(short, long, default_value = "8")]
    key_size: usize,

    /// value size to generate random memcached value
    #[arg(short, long, default_value = "32")]
    value_size: usize,

    /// number of test entries to generate
    #[arg(short, long, default_value = "100000")]
    nums: usize,

    /// udp or tcp protocol for memcached
    #[arg(short = 't', long,default_value_t = Protocol::Udp , value_enum)]
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

// TODO add mutiple thread support
async fn get_command_benchmark(
    server: &memcache::Client,
    test_dict: Arc<HashMap<String, String>>,
    nums: usize,
) -> std::result::Result<(), MemcacheError> {
    let start = std::time::Instant::now();
    let keys: Vec<&String> = test_dict.keys().collect();
    let dict_len = keys.len();

    for _ in 0..nums {
        let rng = rand::thread_rng().gen_range(0..dict_len);
        let key = keys[rng].clone();
        if let Some(value) = test_dict.get(&key) {
            let return_value = server.get::<String>(&key).unwrap();
            assert_eq!(return_value, Some(value.to_string()));
        }
    }

    let duration = start.elapsed();
    println!("Time elapsed in get_command_benchmark() is: {:?}", duration);

    Ok(())
}

#[tokio::main]
async fn main() -> std::result::Result<(), MemcacheError> {
    let args = Cli::parse();

    let server = match args.protocol {
        Protocol::Udp => memcache::connect(format!(
            "memcache+udp://{}:{}?connect_timeout=1",
            args.server_address, args.port
        )),
        Protocol::Tcp => memcache::connect(format!(
            "memcache://{}:{}?connect_timeout=1",
            args.server_address, args.port
        )),
    }?;

    exmaple_method(&server)?;

    let test_dict = generate_memcached_test_dict(args.key_size, args.value_size, args.nums);

    let test_dict = Arc::new(test_dict);

    // assign test_dict to server
    set_memcached_value(&server, test_dict.clone())?;

    // get command benchmark
    get_command_benchmark(&server, test_dict.clone(), args.nums).await?;
    get_command_benchmark(&server, test_dict.clone(), args.nums).await?;

    Ok(())
}

