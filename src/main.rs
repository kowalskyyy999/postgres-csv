use std::{fs::File, io::Read};
use csv;
use dotenv;
use postgres::{NoTls, Config};

fn main() -> Result<(), Box<dyn std::error::Error>> {

    dotenv::from_filename(".env").ok();

    let pg_port: u16 = dotenv::var("PG_PORT").unwrap().parse()?;
    let pg_host = dotenv::var("PG_HOST").unwrap();
    let pg_user = dotenv::var("PG_USER").unwrap();
    let pg_pass = dotenv::var("PG_PASS").unwrap();

    let mut file_csv = csv::Reader::from_path("data/spotify_data.csv")?;

    let mut file_create_table = File::open("db/table.sql").unwrap();
    let mut query_create_table = String::new();
    file_create_table.read_to_string(&mut query_create_table).unwrap();

    let mut file_insert_table = File::open("db/insert.sql").unwrap();
    let mut query_insert_table =  String::new();
    file_insert_table.read_to_string(&mut query_insert_table)?;

    let mut client = Config::new()
        .user(&pg_user)
        .password(&pg_pass)
        .host(&pg_host)
        .port(pg_port)
        .connect(NoTls)?;

    client.batch_execute(&mut query_create_table)?;
    
    for result in file_csv.records() {
        let record = result?;

        let id: i32 = record[0].parse()?;
        let artist_name = &record[1];
        let track_name = &record[2];
        let track_id = &record[3];
        let popularity: i32 = record[4].parse()?;
        let year: i32 = record[5].parse()?;
        let genre = &record[6];
        let danceability: f64 = record[7].parse()?;
        let energy: f64 = record[8].parse()?;
        let key: i32 = record[9].parse()?;
        let loudness: f64 = record[10].parse()?;
        let mode: i32 = record[11].parse()?;
        let speechless: f64 = record[12].parse()?;
        let acousticness: f64 = record[13].parse()?;
        let instrumentalness: f64 = record[14].parse()?;
        let liveness: f64 = record[15].parse()?;
        let valence: f64 = record[16].parse()?;
        let tempo: f64 = record[17].parse()?;
        let duration_ms: i32 = record[18].parse()?;
        let time_signature: i32 = record[19].parse()?;
        // let record: SpotifyRecord = result?;
        
        // // let id = record[0].parse()?;
        // println!("{}", danceability);
        // // println!("{:?}", record);

        client.execute(&mut query_insert_table, &[&id, &artist_name, &track_name, &track_id, &popularity, &year, &genre, &danceability, 
            &energy, &key, &loudness, &mode, &speechless, &acousticness, &instrumentalness, &liveness, &valence, &tempo, &duration_ms, &time_signature],)?;
    };

    Ok(())
}