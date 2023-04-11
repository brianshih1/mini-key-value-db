mod test {

    mod transaction_conflicts {
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
                async fn read_waits_for_uncommitted_write() {
                    let db = Arc::new(DB::new_cleaned("./tmp/data", Timestamp::new(10)));
                    let write_txn = db.begin_txn().await;
                    let key = "foo";
                    db.write(key, 12, write_txn).await;
                    db.set_time(Timestamp::new(12));
                    let read_txn = db.begin_txn().await;

                    let db_1 = db.clone();
                    let task_1 = tokio::spawn(async move {
                        let read_res = db_1.read::<i32>(key, read_txn).await;
                        db_1.commit_txn(read_txn).await;
                        assert_eq!(read_res, Some(12));
                    });

                    let db_2 = db.clone();
                    let task_2 = tokio::spawn(async move {
                        db_2.commit_txn(write_txn).await;
                    });
                    tokio::try_join!(task_1, task_2).unwrap();
                }
            }

            // A read running into an uncommitted intent with a higher timestamp ignores the
            // intent and does not need to wait.
            mod uncommitted_intent_has_higher_timestamp {
                use std::sync::Arc;

                use crate::db::db::{Timestamp, DB};

                #[tokio::test]
                async fn ignores_intent_with_higher_timestamp() {
                    let db = Arc::new(DB::new_cleaned("./tmp/data", Timestamp::new(10)));
                    let read_txn = db.begin_txn().await;
                    let key = "foo";

                    db.set_time(Timestamp::new(12));
                    let write_txn = db.begin_txn().await;
                    db.write(key, 12, write_txn).await;
                    let read_res = db.read::<i32>(key, read_txn).await;
                    assert!(read_res.is_none());
                }
            }
        }

        // A write running into an uncommitted intent with a lower timestamp will wait for the transaction
        // to finish.
        // A write running into a committed value with a higher tiestamp will bump its timestamp.
        mod write_write {
            mod run_into_uncommitted_intent {
                use std::sync::Arc;

                use crate::{
                    db::db::{CommitTxnResult, Timestamp, DB},
                    hlc::{
                        clock::{Clock, ManualClock},
                        timestamp::Timestamp as HLCTimestamp,
                    },
                };

                #[tokio::test]
                async fn write_waits_for_uncommitted_write() {
                    let db = Arc::new(DB::new_cleaned("./tmp/data", Timestamp::new(10)));
                    let txn_1 = db.begin_txn().await;
                    let txn_2 = db.begin_txn().await;
                    let key = "baz";
                    db.write(key, 12, txn_1).await;
                    db.set_time(Timestamp::new(15));

                    let db_1 = db.clone();
                    // txn_2 writes and commits (waits until txn_1 commits)
                    let task_1 = tokio::spawn(async move {
                        db_1.write(key, 100, txn_2).await;
                        let commit_res = db_1.commit_txn(txn_2).await;
                        match commit_res {
                            CommitTxnResult::Success(res) => {
                                assert_eq!(res.commit_timestamp, HLCTimestamp::new(10, 2));
                                res.commit_timestamp
                            }
                            CommitTxnResult::Fail(_) => panic!("failed to commit"),
                        };
                    });

                    // txn_1 commits
                    let db_2 = db.clone();
                    let task_2 = tokio::spawn(async move {
                        let commit_res = db_2.commit_txn(txn_1).await;
                        match commit_res {
                            CommitTxnResult::Success(res) => {
                                assert_eq!(res.commit_timestamp, HLCTimestamp::new(10, 1));
                                res.commit_timestamp
                            }
                            CommitTxnResult::Fail(_) => panic!("failed to commit"),
                        };
                        println!("Finished committing txn_1");
                    });

                    tokio::try_join!(task_1, task_2).unwrap();
                }
            }

            mod run_into_committed_intent {
                use std::sync::Arc;

                use crate::{
                    db::db::{CommitTxnResult, Timestamp, DB},
                    hlc::{
                        clock::{Clock, ManualClock},
                        timestamp::Timestamp as HLCTimestamp,
                    },
                };

                #[tokio::test]
                async fn bump_write_timestamp_before_committing() {
                    let db = Arc::new(DB::new_cleaned("./tmp/data", Timestamp::new(10)));
                    let key = "foo";

                    // begin txn1
                    let write_txn_1 = db.begin_txn().await;

                    // time = 12
                    db.set_time(Timestamp::new(12));

                    // begin txn2. txn2 writes and commits
                    let write_txn_2 = db.begin_txn().await;
                    db.write(key, 12, write_txn_2).await;
                    let txn_2_commit_res = db.commit_txn(write_txn_2).await;
                    let txn_2_commit_timestamp = match txn_2_commit_res {
                        CommitTxnResult::Success(res) => {
                            assert_eq!(res.commit_timestamp.wall_time, 12);
                            res.commit_timestamp
                        }
                        CommitTxnResult::Fail(_) => panic!("failed to commit"),
                    };

                    // txn1 writes
                    db.write(key, 15, write_txn_1).await;

                    // txn1 attempts to commit - it should advance
                    let commit_res = db.commit_txn(write_txn_1).await;

                    match commit_res {
                        CommitTxnResult::Success(res) => {
                            assert_eq!(
                                res.commit_timestamp,
                                txn_2_commit_timestamp.next_logical_timestamp()
                            );
                        }
                        CommitTxnResult::Fail(_) => panic!("failed to commit"),
                    }
                }
            }
        }

        /**
         * If a write detects a read on the same key with a higher timestamp,
         * the writeTimestamp is bumped
         */
        mod read_write {
            use std::sync::Arc;

            use crate::db::db::{CommitTxnResult, Timestamp, DB};

            #[tokio::test]
            async fn bump_write_timestamp_before_committing() {
                let db = Arc::new(DB::new_cleaned("./tmp/data", Timestamp::new(10)));
                let key = "foo";

                let write_txn = db.begin_txn().await;

                db.set_time(Timestamp::new(20));

                let read_txn = db.begin_txn().await;
                db.read::<i32>(key, read_txn).await;

                db.write(key, 100, write_txn).await;

                let write_txn_commit_res = db.commit_txn(write_txn).await;
                match write_txn_commit_res {
                    CommitTxnResult::Success(res) => {
                        assert_eq!(res.commit_timestamp.wall_time, 20);
                    }
                    CommitTxnResult::Fail(_) => panic!("failed to commit"),
                };

                let read_txn_commit_res = db.commit_txn(read_txn).await;
                match read_txn_commit_res {
                    CommitTxnResult::Success(res) => {
                        assert_eq!(res.commit_timestamp.wall_time, 20);
                    }
                    CommitTxnResult::Fail(_) => panic!("failed to commit"),
                };
            }
        }
    }

    mod read_refresh {
        use std::sync::Arc;

        use tokio::time::{self, sleep, Duration};

        mod read_refresh_success {
            use std::sync::Arc;

            use crate::db::db::{CommitTxnResult, Timestamp, DB};

            #[tokio::test]
            async fn read_refresh_from_write_write_conflict() {
                let db = Arc::new(DB::new_cleaned("./tmp/data", Timestamp::new(10)));

                let read_key = "foo";
                let write_key = "bar";

                let txn_1 = db.begin_txn().await;
                db.read::<i32>(read_key, txn_1).await;

                db.set_time(Timestamp::new(15));

                // txn_2 writes and commits
                let txn_2 = db.begin_txn().await;
                db.write(write_key, "foo", txn_2).await;
                db.commit_txn(txn_2).await;

                // txn_1 writes - timestamp gets advanced to 15 due to
                // write-write conflict
                db.write(write_key, "bar", txn_1).await;

                let res = db.commit_txn(txn_1).await;
                match res {
                    CommitTxnResult::Success(res) => {
                        assert_eq!(res.commit_timestamp.wall_time, 15);
                    }
                    CommitTxnResult::Fail(_) => panic!("failed to commit"),
                };
            }
        }

        // Advancing a transaction read timestamp from ta to tb is possible
        // if we can prove that none of the data
        mod read_refresh_failure {
            use std::sync::Arc;

            use crate::db::db::{CommitTxnResult, Timestamp, DB};

            #[tokio::test]
            async fn read_refresh_failure() {
                let db = Arc::new(DB::new_cleaned("./tmp/data", Timestamp::new(10)));

                let read_key = "foo";
                let write_key = "bar";

                let txn_1 = db.begin_txn().await;
                db.read::<i32>(read_key, txn_1).await;

                db.set_time(Timestamp::new(15));

                // txn_2 writes and commits
                let txn_2 = db.begin_txn().await;
                db.write(write_key, "foo", txn_2).await;
                db.write(read_key, 12, txn_2).await;
                let txn_2_res = db.commit_txn(txn_2).await;
                match txn_2_res {
                    CommitTxnResult::Success(res) => {
                        assert_eq!(res.commit_timestamp.wall_time, 15);
                    }
                    CommitTxnResult::Fail(_) => panic!("failed to commit"),
                };

                // txn_1 writes - timestamp gets advanced to 15 due to
                // write-write conflict
                db.write(write_key, "bar", txn_1).await;

                let res = db.commit_txn(txn_1).await;
                match res {
                    CommitTxnResult::Success(res) => {
                        assert_eq!(res.commit_timestamp.wall_time, 15);
                    }
                    CommitTxnResult::Fail(_) => panic!("failed to commit"),
                };
            }
        }
    }

    mod abort_txn {
        use crate::db::db::{Timestamp, DB};

        #[tokio::test]
        async fn read_write_after_abort_transaction() {
            let db = DB::new_cleaned("./tmp/data", Timestamp::new(10));
            let key = "foo";

            let txn_to_abort = db.begin_txn().await;
            db.write(key, 12, txn_to_abort).await;
            db.abort_txn(txn_to_abort).await;

            let read_txn = db.begin_txn().await;
            let res = db.read::<i32>(key, read_txn).await;
            db.abort_txn(read_txn).await;
            assert!(res.is_none());

            let write_txn = db.begin_txn().await;
            db.write(key, 100, write_txn).await;
            println!("Foooo");
            db.commit_txn(write_txn).await;
            println!("ENDDD");
        }
    }

    mod deadlock {
        #[tokio::test]
        async fn test() {}
    }

    mod run_txn {
        use std::sync::Arc;

        use crate::db::db::{Timestamp, DB};

        #[tokio::test]
        async fn reading_its_txn_own_write() {
            let db = DB::new_cleaned("./tmp/data", Timestamp::new(10));
            db.run_txn(|txn_context| async move {
                let key = "foo";
                txn_context.write(key, 12).await;
                let read = txn_context.read::<i32>(key).await;
                assert_eq!(read, Some(12));
            })
            .await;
        }

        #[tokio::test]
        async fn writing_its_own_write() {
            let db = DB::new_cleaned("./tmp/data", Timestamp::new(10));
            db.run_txn(|txn_context| async move {
                let key = "foo";
                txn_context.write(key, 12).await;
                txn_context.write(key, 100).await;
                let read = txn_context.read::<i32>(key).await;
                assert_eq!(read, Some(100));
            })
            .await;
        }

        #[tokio::test]
        async fn two_concurrent_writes_to_same_key() {
            let db = Arc::new(DB::new_cleaned("./tmp/data", Timestamp::new(10)));

            let db_1 = db.clone();
            let key = "foo";
            let task_1 = tokio::spawn(async move {
                db_1.run_txn(|txn_context| async move {
                    txn_context.write(key, 88).await;
                    let read = txn_context.read::<i32>(key).await;
                    println!("txn1 finished");
                    assert_eq!(read, Some(88));
                })
                .await
            });

            let db_2 = db.clone();
            let task_2 = tokio::spawn(async move {
                db_2.run_txn(|txn_context| async move {
                    txn_context.write(key, 12).await;
                    let read = txn_context.read::<i32>(key).await;
                    println!("txn2 finished");
                    assert_eq!(read, Some(12));
                })
                .await;
            });
            tokio::try_join!(task_1, task_2).unwrap();
        }
    }
}
