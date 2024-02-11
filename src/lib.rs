use std::io;

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
    pub fn from_csv<T: io::Read>(
        name: String,
        mut reader: csv::Reader<T>,
    ) -> Result<Table, String> {
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
}
