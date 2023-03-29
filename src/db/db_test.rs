mod test {
    use crate::db::db::DB;

    #[tokio::test]
    async fn test() {
        let db = DB::new("./tmp/data");
    }
}
