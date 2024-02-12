use clap::Parser;
use std::path;
use std::process;
use qcsv::Table;
use qcsv::Output;

/// Simple program to turn a csv file into a sqlite database
#[derive(Parser, Default)]
struct Args {
    /// Input CSV files
    infiles: Vec<path::PathBuf>,

    /// Output sqlite file
    #[arg(short, long)]
    outfile: Option<path::PathBuf>,
}

fn main() {
    let args = Args::parse();

    let output: Output = match args.outfile {
        None => Output::Stdout,
        Some(outfile) => match Output::from_argument(&outfile) {
            Ok(out) => out,
            Err(err) => {
                eprintln!("{err}");
                process::exit(1);
            }
        },
    };

    for infile in args.infiles {
        let table = Table::from_csv(&infile).unwrap();
        let query = table.to_sql_query();
        output.write(query);
    }
}
