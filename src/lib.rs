use std::{
    fs, io,
    path::PathBuf,
};

pub struct Table {
    pub name: String,
    pub header: csv::StringRecord,
    pub records: Vec<csv::StringRecord>,
}

impl Table {
    pub fn to_sql_query(&self) -> String {
        let table_name = &self.name;
        let columns = self
            .header
            .iter()
            .map(|col| format!("\"{}\" TEXT", String::from(col)))
            .collect::<Vec<String>>()
            .join(", ");

        let mut query = format!("CREATE TABLE {table_name} ({columns});\n");

        let values_query = self
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
        return query;
    }

    // TODO just take in the filename
    pub fn from_csv(csv_path: &PathBuf) -> Result<Table, String> {
        let f = match fs::File::open(csv_path) {
            Ok(file) => file,
            Err(error) => { return Err(error.to_string()); }
        };

        let mut reader = csv::ReaderBuilder::new()
            .has_headers(false)
            .from_reader(io::BufReader::new(f));

        let table_name = csv_path
            .file_stem()
            .unwrap()
            .to_os_string()
            .into_string()
            .unwrap();

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
            name: table_name,
            header,
            records,
        });
    }
}


pub enum Output {
    Stdout,
    Sqlite(sqlite::Connection),
}

impl Output {
    pub fn from_argument(outfile: &PathBuf) -> Result<Self, sqlite::Error> {
        sqlite::open(outfile).map(|conn| Self::Sqlite(conn))
    }

    pub fn write(&self, string: String) {
        match self {
            Self::Stdout => println!("{string}"),
            Self::Sqlite(conn) => {
                if let Err(err) = conn.execute(string) {
                    eprintln!("{err}")
                }
            }
        }
    }
}

