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



//##:¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤
//##:¤¤¤¤¤¤¤¤¤¤¤ Constants
//##:¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤
const USERAGENT: &str = concat!("Slurpy (PodcastIndex.org)/v", env!("CARGO_PKG_VERSION"));
const DB_FILE_PATH: &str = "/mnt/c/temp/podcastindex_feeds.db";
const OUTPUT_FOLDER: &str = "/mnt/d/enclosures";
const MAX_ENCLOSURES_PER_ROUND: usize = 23;



//##:¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤
//##:¤¤¤¤¤¤¤¤¤¤¤ Structs
//##:¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤
struct Podcast {
    id: usize,
    enclosure: Enclosure,
    client: reqwest::Client
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
//##:¤¤¤¤¤¤¤¤¤¤¤ Main()
//##:¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤
#[tokio::main]
async fn main() {
    let mut rng = rand::thread_rng();

    //Announce what we are
    println!("{}", USERAGENT);
    println!("{}\n", "-".repeat(USERAGENT.len()));

    //Build the query client
    let mut headers = header::HeaderMap::new();
    headers.insert("User-Agent", header::HeaderValue::from_static(USERAGENT));
    let client = reqwest::Client::builder()
        .use_rustls_tls()
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(30))
        .pool_idle_timeout(Duration::from_secs(20))
        .default_headers(headers)
        .gzip(true)
        .build()
        .unwrap();

    // This vector will hold the podcasts we are going to download enclosure
    // for.  It will be a vector of Podcast structs.
    let mut count: usize = 0;
    let mut start_at: usize = 0;
    loop {
        match get_feeds_from_sql(&DB_FILE_PATH.to_string(), start_at, MAX_ENCLOSURES_PER_ROUND, &client) {
            Ok(podcasts) => {
                //Kill the loop if nothing returns
                if podcasts.len() == 0 {
                    println!("Downloaded: [{}] enclosures.", count);
                    break;
                }

                //Store the last id for where to start on the next iteration
                start_at = podcasts.last().unwrap().id + 1;

                //Attempt to download this batch of enclosures
                match fetch_enclosures(podcasts).await {
                    Ok(_) => {
                        count += 1;
                    }
                    Err(_) => {}
                }
            }
            Err(e) => {
                eprintln!("Could not get a list of podcasts to download. Error: {}", e);
            }
        }


        //Cool down
        let cooldown_seconds = rng.gen_range(33..120);
        println!("Pausing [{}] seconds for cooldown...", cooldown_seconds);
        std::thread::sleep(std::time::Duration::from_secs(cooldown_seconds));
    }

    println!("Downloaded: [{}] enclosures.", count);
}



//##:¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤
//##:¤¤¤¤¤¤¤¤¤¤¤ Functions()
//##:¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤

//##: Take in a vector of Podcasts and attempt to pull each one of them that is update
async fn fetch_enclosures(podcasts: Vec<Podcast>) -> Result<(), Box<dyn std::error::Error>> {

    let fetches = futures::stream::iter(
        podcasts.into_iter().map(|podcast| {
            async move {
                let url = &podcast.enclosure.url;
                let enclosure_path = format!("{}/{}.mp3", OUTPUT_FOLDER, podcast.id);
                if std::path::Path::new(&enclosure_path).exists() {
                    println!("Skipping: [{}|{}]... File exists.", podcast.id, podcast.enclosure.duration);
                } else {
                    print!("Retrieving: [{}|{}|{}]... ", podcast.id, podcast.enclosure.duration, url);

                    if let Ok(response) = podcast.client.get(url).send().await {
                        if response.status().is_success() {
                            println!("Success!");

                            let mut file = File::create(&enclosure_path).unwrap();

                            let mut byte_stream = response.bytes_stream();

                            while let Some(item) = byte_stream.next().await {
                                if Write::write_all(&mut file, &item.unwrap()).is_err() {
                                    eprintln!("Error writing file for: {}", podcast.enclosure.url);
                                }
                            }
                        } else if response.status().is_server_error() {
                            println!("Server Error!");
                        } else {
                            println!("Error. Status: [{:?}]", response.status());
                        }
                    }
                }
            }
        })
    ).buffer_unordered(MAX_ENCLOSURES_PER_ROUND).collect::<Vec<()>>();
    fetches.await;
    Ok(())
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
                client: client.clone()
            })
        }).unwrap();

    //Parse the results
    for row in rows {
        let podcast: Podcast = row.unwrap();
        podcasts.push(podcast);
    }

    Ok(podcasts)
}
