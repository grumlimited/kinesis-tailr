use crate::sink::file::check_path;

#[tokio::test]
async fn check_path_ok() {
    let path = "test.txt";

    assert_eq!(check_path(path).await.unwrap(), ());
}

#[tokio::test]
#[should_panic]
async fn check_path_nok() {
    let path = "/tmp";

    check_path(path).await.unwrap();
}
