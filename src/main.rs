use std::path::{Path, PathBuf};

use gumdrop::Options;
use log::{LevelFilter, info};
use postgres::{Client, NoTls};
use serde::Deserialize;

#[derive(Debug, Options)]
struct CommandOptions {
    #[options(help = "print help message")]
    help: bool,
    #[options(help = "be verbose")]
    verbose: bool,

    #[options(command)]
    command: Option<Command>,
}

#[derive(Debug, Options)]
enum Command {
    #[options(help = "insert bdfr json archive into Lemmy DB")]
    Import(ImportOptions),
}

#[derive(Debug, Options)]
struct ImportOptions {
    #[options(help = "archive folder path", required)]
    archive_path: PathBuf,

    #[options(help = "postgres password")]
    postgres_pass: String,
}

#[derive(Debug, Deserialize)]
pub struct RedditPost {
    pub title: String,
    pub name: String,
    pub url: String,
    pub selftext: String,
    pub score: i32,
    pub upvote_ratio: f32,
    pub permalink: String,
    pub id: String,
    pub author: String,
    pub link_flair_text: Option<String>,
    pub num_comments: i32,
    pub over_18: bool,
    pub spoiler: bool,
    pub pinned: bool,
    pub locked: bool,
    pub distinguished: Option<String>,
    pub created_utc: f64,
    pub comments: Vec<Comment>,
}

#[derive(Debug, Deserialize)]
pub struct Comment {
    pub author: String,
    pub id: String,
    pub score: i32,
    pub subreddit: String,
    pub author_flair: Option<String>,
    pub submission: String,
    pub stickied: bool,
    pub body: String,
    pub is_submitter: bool,
    pub distinguished: Option<String>,
    pub created_utc: f64,
    pub parent_id: String,
    pub replies: Vec<Comment>, // Recursively nested replies
}

fn main() {
    let opts = CommandOptions::parse_args_default_or_exit();

    let mut builder = env_logger::Builder::new();

    if opts.verbose {
        builder.filter_level(LevelFilter::Debug);
    } else {
        builder.filter_level(LevelFilter::Info);
    }
    builder.init();

    if let Some(Command::Import(ref import_options)) = opts.command {
        let client = Client::connect(
            "postgresql://postgres:postgres@localhost:5432/postgres",
            NoTls,
        )
        .expect("Unable to connect to postgres db!");

        let path = Path::new(&import_options.archive_path);

        info!("Inserting archives inside {} into Lemmy DB", path.display());
    }
}
