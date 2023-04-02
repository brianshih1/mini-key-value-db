mod test {
    use crate::db::db::{Timestamp, DB};

    #[tokio::test]
    async fn test() {
        let db = DB::new("./tmp/data", 10);
        let txn_1 = db.begin_txn().await;
        db.write("foo", 12, txn_1).await;
        db.commit_txn(txn_1).await;

        let res = db
            .read_without_txn::<i32>("hello", Timestamp::new(11))
            .await;
        println!("Result is: {}", res)
    }

    // #[tokio::test]
    // async fn test_run_txn() {
    //     let db = DB::new("./tmp/data", 10);
    //     let txn_1 = db.begin_txn().await;
    //     db.write("foo", 12, txn_1).await;
    //     db.commit_txn(txn_1).await;

    //     let res = db
    //         .read_without_txn::<i32>("hello", Timestamp::new(11))
    //         .await;
    //     println!("Result is: {}", res);
    //     db.run_txn(|db| async { true }).await;
    // }
}
