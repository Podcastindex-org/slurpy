//##:¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤
//##:¤¤¤¤¤¤¤¤¤¤¤ Imports & Modules
//##:¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤
use std::error::Error;
use std::fmt;
use reqwest;
use rusqlite::{Connection};
use std::fs::File;
use std::io::Write;
use futures_util::StreamExt;



//##:¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤
//##:¤¤¤¤¤¤¤¤¤¤¤ Constants
//##:¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤
const DB_FILE_PATH: &str = "/mnt/c/temp/podcastindex_feeds.db";
const OUTPUT_FOLDER: &str = "/mnt/d/enclosures";
const MAX_ENCLOSURES_PER_ROUND: usize = 23;



//##:¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤
//##:¤¤¤¤¤¤¤¤¤¤¤ Structs
//##:¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤
struct Podcast {
    id: usize,
    enclosure: Enclosure,
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

    // This vector will hold the podcasts we are going to download enclosure
    // for.  It will be a vector of Podcast structs.
    let mut count: usize = 0;
    let mut start_at: usize = 0;
    loop {
        match get_feeds_from_sql(&DB_FILE_PATH.to_string(), start_at, MAX_ENCLOSURES_PER_ROUND) {
            Ok(podcasts) => {
                //Kill the loop if nothing returns
                if podcasts.len() == 0 {
                    break;
                }

                for podcast in podcasts {
                    match download_enclosure(podcast).await {
                        Ok(_) => {
                            count += 1;
                            println!("Successfully downloaded.");
                        }
                        Err(e) => {
                            eprintln!("Could not download file. Error: {}", e);
                        }
                    }
                }
            }
            Err(e) => {
                eprintln!("Could not get a list of podcasts to download. Error: {}", e);
            }
        }

        //Keep a running total, which is also the starting point for the next db grab
        start_at += 1;

        //Cool down
        std::thread::sleep(std::time::Duration::from_secs(13));
    }

    println!("Downloaded: [{}] enclosures.", count);
}



//##:¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤
//##:¤¤¤¤¤¤¤¤¤¤¤ Functions()
//##:¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤¤
async fn download_enclosure(podcast: Podcast) -> Result<bool, Box<dyn Error>> {


    //DEBUG: grab the first enclosure
    let url = &podcast.enclosure.url;
    let mut file = File::create(format!("{}/{}.mp3", OUTPUT_FOLDER, podcast.id)).unwrap();

    println!("Retrieving: [{}|{}|{}]", podcast.id, podcast.enclosure.duration, podcast.enclosure.url);

    if let Ok(response) = reqwest::get(url).await {
        if response.status().is_success() {
            println!("success!");

            let mut byte_stream = response.bytes_stream();

            while let Some(item) = byte_stream.next().await {
                if Write::write_all(&mut file, &item.unwrap()).is_err() {
                    eprintln!("Error writing file for: {}", podcast.enclosure.url);
                }
            }
        } else if response.status().is_server_error() {
            println!("server error!");
        } else {
            println!("Something else happened. Status: {:?}", response.status());
        }
    }

    Ok(true)
}


//Connect to the database at the given file location
fn connect_to_database(filepath: &String) -> Result<Connection, Box<dyn Error>> {
    if let Ok(conn) = Connection::open(filepath.as_str()) {
        Ok(conn)
    } else {
        return Err(Box::new(HydraError(format!("Could not open a database file at: [{}].", filepath).into())))
    }
}


//##: Get a list of podcasts from the downloaded sqlite db
fn get_feeds_from_sql(filepath: &String, index: usize, max: usize) -> Result<Vec<Podcast>, Box<dyn Error>> {
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
    let rows = stmt.query_map(&[(":index", index.to_string().as_str()), (":max", max.to_string().as_str())], |row| {
        Ok(Podcast {
            id: row.get(0).unwrap(),
            enclosure: Enclosure {
                url: row.get(1).unwrap(),
                duration: row.get(2).unwrap_or(0),
            },
        })
    }).unwrap();

    //Parse the results
    for row in rows {
        let podcast: Podcast = row.unwrap();
        podcasts.push(podcast);
    }

    Ok(podcasts)
}