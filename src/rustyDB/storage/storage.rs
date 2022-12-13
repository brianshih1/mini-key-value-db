use rocksdb::{ColumnFamily, DBIterator, Error, Options, DB};

use crate::RustyError;

pub struct Storage {
    db: rocksdb::DB,
}

pub type RustyResult<T> = Result<T, RustyError>;

impl Storage {
    pub fn new(path: &str) -> Storage {
        return Storage {
            db: DB::open_default(path).unwrap(),
        };
    }

    fn create_column_family(&mut self, cf_name: &str) -> Result<(), Error> {
        let options = rocksdb::Options::default();
        self.db.create_cf(cf_name, &options)
    }

    fn get_column_family(&mut self, cf_name: &str) -> RustyResult<&ColumnFamily> {
        self.db
            .cf_handle(cf_name)
            .ok_or(RustyError::new("".to_owned()))
    }

    pub fn put<V>(&mut self, cf_name: &str, key: &str, value: V) -> Result<(), RustyError>
    where
        V: AsRef<[u8]>,
    {
        self.get_column_family(cf_name).and_then(|cf| {
            self.db
                .put_cf(cf, key, value)
                .map_err(|e| RustyError::new("".to_owned()))
        })
    }

    pub fn get_preseek_iterator(
        &mut self,
        cf_name: &str,
        prefix_name: &str,
    ) -> Result<DBIterator, RustyError> {
        let cf_handle = self.db.cf_handle(cf_name);
        match cf_handle {
            Some(cf) => Ok(self.db.prefix_iterator_cf(cf, prefix_name)),
            None => Err(RustyError::new("no cfHandle found for prefix".to_owned())),
        }
    }
}
