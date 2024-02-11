use clap::Parser;
use std::fs;
use std::io;
use std::path;
use std::process;

/// Simple program to turn a csv file into a sqlite database
#[derive(Parser, Debug)]
struct Args {
    /// Input CSV file
    #[arg(short, long)]
    infile: path::PathBuf,

    /// Output sqlite file
    #[arg(short, long)]
    outfile: path::PathBuf,
}

fn main() {
    let args = Args::parse();

    let f = match fs::File::open(&args.infile) {
        Ok(file) => file,
        Err(error) => {
            eprintln!("{error}");
            process::exit(1);
        }
    };

    let reader = csv::ReaderBuilder::new()
        .has_headers(false)
        .from_reader(io::BufReader::new(f));

    let table_name = args
        .infile
        .file_stem()
        .unwrap()
        .to_os_string()
        .into_string()
        .unwrap();

    let table = qcsv::Table::from_csv(table_name, reader).unwrap();
    let query = table.to_sql_query();

    if args.outfile.as_os_str() == "-" {
        println!("{query}");
    }

    let conn = match sqlite::open(&args.outfile) {
        Ok(conn) => conn,
        Err(err) => {
            eprintln!("{err}");
            process::exit(1);
        }
    };

    if let Err(err) = conn.execute(query) {
        eprintln!("{err}");
        process::exit(1);
    }
}
