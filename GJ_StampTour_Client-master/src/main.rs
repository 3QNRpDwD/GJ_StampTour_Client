use reqwest;
use std::{collections::HashMap, io::{self, Write}};
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use tokio; // 추가된 부분

#[serde_as]
#[derive(Serialize, Deserialize, Debug, Clone)]
struct Command {
    command: String,
    output: String,
}

#[tokio::main] // 추가된 부분
async fn main() {
    handle_command().await;
}

async fn handle_command() {
    loop {

        let client = reqwest::Client::new();

        // 프롬프트를 출력하고 입력을 받습니다.
        print!("Server command: ");
        io::stdout().flush().unwrap();

        let mut buffer = String::new();
        io::stdin().read_line(&mut buffer).expect("Failed to read line");

        let mut command_map = HashMap::new();
        command_map.insert("command", buffer.trim());
        command_map.insert("output", "");

        let res = client.post("http://127.0.0.1/admin")
            .json(&command_map)
            .send()
            .await?;

        println!("{:?}", res.unwrap().json());
        // 입력 내용을 출력합니다.
        println!("Command sent successfully!");

    }
}
