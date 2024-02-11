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

    let table = parse_csv(table_name, reader).unwrap();

    let conn = match sqlite::open(args.outfile) {
        Ok(conn) => conn,
        Err(err) => {
            eprintln!("{err}");
            process::exit(1);
        }
    };

    match create_table(table, conn) {
        Ok(()) => process::exit(0),
        Err(err) => {
            eprintln!("{err}");
            process::exit(1);
        }
    }
}

struct Table {
    name: String,
    header: csv::StringRecord,
    records: Vec<csv::StringRecord>,
}

fn parse_csv<T: io::Read>(name: String, mut reader: csv::Reader<T>) -> Result<Table, String> {
    let Some(Ok(header)) = reader.records().next() else {
        return Err("CSV: Could not read header. File may be empty".to_string());
    };

    let mut records = Vec::new();

    for result in reader.records() {
        match result {
            Ok(x) => records.push(x),
            Err(err) => return Err(err.to_string()),
        }
    }
    return Ok(Table {
        name,
        header,
        records,
    });
}

fn create_table(table: Table, conn: sqlite::Connection) -> Result<(), String> {
    let table_name = table.name;
    let columns = table
        .header
        .iter()
        .map(|col| format!("\"{}\" TEXT", String::from(col)))
        .collect::<Vec<String>>()
        .join(", ");

    let mut query = format!("CREATE TABLE {table_name} ({columns});\n");

    let values_query = table
        .records
        .iter()
        .map(|row| {
            let values = row
                .iter()
                .map(|value| value.replace("'", "''"))
                .map(|value| format!("'{value}'"))
                .collect::<Vec<String>>()
                .join(", ");
            format!("INSERT INTO {table_name} VALUES ({values});")
        })
        .collect::<Vec<String>>()
        .join("\n");

    query.push_str(&values_query);

    conn.execute(query).map_err(|err| err.to_string())
}
