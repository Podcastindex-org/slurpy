//##:¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤
//##:¤¤¤¤¤¤¤¤¤¤¤ Imports & Modules
//##:¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤
use std::error::Error;
use std::fmt;
use reqwest::header;
use rusqlite::{Connection};
use std::fs::File;
use std::io::Write;
use futures_util::StreamExt;
use rand::Rng;
use std::time::{Duration};
use clap::Parser;


//##:¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤
//##:¤¤¤¤¤¤¤¤¤¤¤ Constants
//##:¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤
const USERAGENT: &str = concat!("Slurpy (PodcastIndex.org)/v", env!("CARGO_PKG_VERSION"));


//##:¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤
//##:¤¤¤¤¤¤¤¤¤¤¤ Structs
//##:¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤
struct Podcast {
    id: usize,
    enclosure: Enclosure,
    client: reqwest::Client,
}

struct Enclosure {
    url: String,
    duration: usize,
}

#[derive(Debug)]
struct HydraError(String);


//##:¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤
//##:¤¤¤¤¤¤¤¤¤¤¤ Traits
//##:¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤
impl fmt::Display for HydraError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Fatal error: {}", self.0)
    }
}

impl Error for HydraError {}


//##:¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤
//##:¤¤¤¤¤¤¤¤¤¤¤ Args
//##:¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤
#[derive(Parser)]
#[clap(author, version, about, long_about = None)]
struct Args {
    // Path to the podcast index database file
    #[clap(short, long, value_parser)]
    db_file_path: String,

    // Path to the output folder
    #[clap(short, long, value_parser)]
    output_folder_path: String,

    // Maximum number of enclosures to download simultaneously
    #[clap(short, long, value_parser, default_value_t = 23)]
    max_enclosures_per_round: usize,

    // What feed id number to start at
    #[clap(short, long, value_parser, default_value_t = 1)]
    start_at_id: usize,

}


//##:¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤
//##:¤¤¤¤¤¤¤¤¤¤¤ Main()
//##:¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤
#[tokio::main]
async fn main() {
    let mut rng = rand::thread_rng();

    //Get args
    let args = Args::parse();

    //Env
    // if let (max_concurrent_downloads) = args.max_enclosures_per_round {
    //
    // }

    //Announce what we are
    println!("{}", USERAGENT);
    println!("{}\n", "-".repeat(USERAGENT.len()));


    // This vector will hold the podcasts we are going to download enclosure
    // for.  It will be a vector of Podcast structs.
    let mut count: usize = 0;
    let mut start_at = args.start_at_id;
    loop {

        //Build the query client
        let mut headers = header::HeaderMap::new();
        headers.insert("User-Agent", header::HeaderValue::from_static(USERAGENT));
        let client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(30))
            .pool_idle_timeout(Duration::from_secs(20))
            .default_headers(headers)
            .gzip(true)
            .build()
            .unwrap();

        //Cool down
        let mut cooldown_seconds: usize = args.max_enclosures_per_round;
        if count == 0 {
            cooldown_seconds = 0;
        }

        println!("[{}]: Getting podcasts from the database...", chrono::offset::Local::now());

        match get_feeds_from_sql(&args.db_file_path, start_at, args.max_enclosures_per_round, &client) {
            Ok(podcasts) => {
                //Kill the loop if nothing returns
                if podcasts.len() == 0 {
                    println!("Downloaded: [{}] enclosures.", count);
                    break;
                }

                //Store the last id for where to start on the next iteration
                start_at = podcasts.last().unwrap().id + 1;

                //Attempt to download this batch of enclosures
                println!("[{}]: Fetching enclosures for: [{}] podcasts.", chrono::offset::Local::now(), podcasts.len());
                match fetch_enclosures(podcasts, &args.output_folder_path).await {
                    Ok(downloaded) => {
                        count += downloaded;
                        cooldown_seconds = downloaded;
                        if downloaded == args.max_enclosures_per_round {
                            cooldown_seconds = rng.gen_range(args.max_enclosures_per_round..90);
                        }
                    }
                    Err(_) => {}
                }
            }
            Err(e) => {
                eprintln!("Could not get a list of podcasts to download. Error: {}", e);
            }
        }

        //Cooldown
        println!("Pausing [{}] seconds for cooldown...", cooldown_seconds);
        std::thread::sleep(std::time::Duration::from_secs(cooldown_seconds as u64));

        drop(client);
    }

    println!("Downloaded: [{}] enclosures.", count);
}


//##:¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤
//##:¤¤¤¤¤¤¤¤¤¤¤ Functions()
//##:¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤

//##: Take in a vector of Podcasts and attempt to pull each one of them that is update
async fn fetch_enclosures(podcasts: Vec<Podcast>, output_folder: &String) -> Result<usize, Box<dyn std::error::Error>> {
    let podcasts_count = podcasts.len();


    let fetches = futures::stream::iter(
        podcasts.into_iter().map(|podcast| {
            async move {
                let mut downloads: usize = 0;
                let url = &podcast.enclosure.url;
                let enclosure_path = format!("{}/{}.mp3", output_folder, podcast.id);
                let mut error_enclosure_path = format!("{}/{}.err", output_folder, podcast.id);

                if std::path::Path::new(&enclosure_path).exists() ||
                   std::path::Path::new(&error_enclosure_path).exists() {
                    println!("Skipping: [{}|{}]... File exists.",
                             podcast.id,
                             podcast.enclosure.duration);
                } else {
                    downloads += 1;
                    println!("Retrieving: [{}|{}|{}]... ",
                             podcast.id,
                             podcast.enclosure.duration,
                             url);

                    if let Ok(response) = podcast.client.get(url).send().await {
                        let rstatus = response.status().as_u16();

                        if response.status().is_success() {
                            let mut file = File::create(&enclosure_path).unwrap();

                            let mut byte_stream = response.bytes_stream();

                            while let Some(item) = byte_stream.next().await {
                                match item {
                                    Ok(stream_chunk) => {
                                        if Write::write_all(&mut file, &stream_chunk).is_err() {
                                            eprintln!("Error writing file for: {}", podcast.enclosure.url);
                                            let _file = File::create(&error_enclosure_path).unwrap();
                                            ()
                                        }
                                    }
                                    Err(_) => {
                                        //eprintln!("Error getting byte stream: [{:?}]", e);
                                        let _file = File::create(&error_enclosure_path).unwrap();
                                        ()
                                    }
                                }
                            }
                        } else {
                            let _file = File::create(&error_enclosure_path).unwrap();
                            error_enclosure_path = format!("{}/{}.{}", output_folder, podcast.id, rstatus);
                            let _file = File::create(&error_enclosure_path).unwrap();
                            eprintln!("Error. Status: [{}|{}|{}]",
                                      podcast.id,
                                      podcast.enclosure.duration,
                                      response.status());
                        }
                    } else {
                        let _file = File::create(&error_enclosure_path).unwrap();
                        println!("Error sending request for [{}].", podcast.id);
                        ()
                    }
                }

                downloads
            }
        })
    ).buffer_unordered(podcasts_count).collect::<Vec<usize>>();
    let returned = fetches.await;
    println!("{:#?}", returned);

    Ok(returned.iter().sum::<usize>() as usize)
}


//Connect to the database at the given file location
fn connect_to_database(filepath: &String) -> Result<Connection, Box<dyn Error>> {
    if let Ok(conn) = Connection::open(filepath.as_str()) {
        Ok(conn)
    } else {
        return Err(Box::new(HydraError(format!("Could not open a database file at: [{}].", filepath).into())));
    }
}


//##: Get a list of podcasts from the downloaded sqlite db
fn get_feeds_from_sql(filepath: &String, index: usize, max: usize, client: &reqwest::Client) -> Result<Vec<Podcast>, Box<dyn Error>> {
    let conn = connect_to_database(filepath)?;
    let mut podcasts: Vec<Podcast> = Vec::new();

    //Run the query and store the result
    let sqltxt: String = format!(
        "SELECT id,
                newestEnclosureUrl,
                newestEnclosureDuration
        FROM podcasts
        WHERE id >= :index
          AND newestEnclosureDuration <= 1440
          AND host NOT LIKE '%anchor.fm%'
          AND host NOT LIKE '%librivox%'
          AND host NOT LIKE '%afr.net%'
          AND host NOT LIKE '%afa.net%'
        ORDER BY id ASC
        LIMIT :max;"
    );

    //Prepare and execute the query
    let mut stmt = conn.prepare(sqltxt.as_str())?;
    let rows = stmt.query_map(
        &[(":index", &index.to_string()), (":max", &max.to_string())], |row| {
            Ok(Podcast {
                id: row.get(0).unwrap(),
                enclosure: Enclosure {
                    url: row.get(1).unwrap(),
                    duration: row.get(2).unwrap_or(0),
                },
                client: client.clone(),
            })
        }).unwrap();

    //Parse the results
    for row in rows {
        let podcast: Podcast = row.unwrap();
        podcasts.push(podcast);
    }

    Ok(podcasts)
}
