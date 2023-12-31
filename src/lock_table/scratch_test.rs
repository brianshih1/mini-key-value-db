mod Test {
    use tokio::time::{sleep, Duration};

    // #[tokio::test]
    async fn try_join() {
        let task_1 = tokio::spawn(async move {
            for i in 1..10 {
                println!("first thread: {}", i);
                sleep(Duration::from_millis(1)).await;
            }
        });

        let task_2 = tokio::spawn(async move {
            for i in 1..10 {
                println!("second thread: {}", i);
                sleep(Duration::from_millis(2)).await;
            }
        });
        tokio::try_join!(task_1, task_2).unwrap();
    }
}
