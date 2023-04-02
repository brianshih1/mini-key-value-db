mod test {
    use crate::db::db::{Timestamp, DB};

    // A read running into an uncommitted intent with a lower timestamp will wait for the
    // earlier transaction

    /**
     * A read running into an uncommitted intent with a lower timestamp will
     * wait for the earlier transaction to finalize.
     *
     * A read running into an uncommitted intent with a higher timestamp ignores the
     * intent and does not need to wait.
     */
    mod write_read {
        mod uncommitted_intent_has_lower_timestamp {

            use std::sync::Arc;

            use tokio::time::{self, sleep, Duration};

            use crate::db::db::{Timestamp, DB};

            #[tokio::test]
            async fn test() {
                let db = Arc::new(DB::new("./tmp/data", Timestamp::new(10)));
                let write_txn = db.begin_txn().await;
                let key = "foo";
                db.write(key, 12, write_txn).await;
                db.set_time(Timestamp::new(12));
                let read_txn = db.begin_txn().await;

                let db_1 = db.clone();
                let task_1 = tokio::spawn(async move {
                    let read_res = db_1.read::<i32>(key, read_txn).await;
                    db_1.commit_txn(write_txn).await;
                    assert_eq!(read_res, Some(12));
                });

                let db_2 = db.clone();
                let task_2 = tokio::spawn(async move {
                    db_2.commit_txn(write_txn).await;
                });
                tokio::try_join!(task_1, task_2).unwrap();
            }
        }

        mod uncommitted_intent_has_higher_timestamp {}
    }

    mod write_write {}

    mod read_write {}

    #[tokio::test]
    async fn test() {
        let db = DB::new("./tmp/data", Timestamp::new(10));
        let txn_1 = db.begin_txn().await;
        db.write("foo", 12, txn_1).await;
        let did_commit = db.commit_txn(txn_1).await;

        println!("Result is: {}", did_commit)
    }

    #[tokio::test]
    async fn test_2() {
        let db = DB::new("./tmp/data", Timestamp::new(10));
        let txn_1 = db.begin_txn().await;
        db.write("foo", 12, txn_1).await;
        db.read::<i32>("hello", txn_1).await;

        db.set_time(Timestamp::new(12));
        let txn_2 = db.begin_txn().await;
        db.write("hello", 100, txn_2).await;
        db.write("bar", 100, txn_2).await;
        db.commit_txn(txn_2).await;

        db.write("bar", 12, txn_1).await;

        let did_txn_1_commit = db.commit_txn(txn_1).await;

        println!("txn_1 commit status: {}", did_txn_1_commit)
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
