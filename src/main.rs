use std::{
    env,
    fs::File,
    io::BufReader,
    path::{Path, PathBuf},
};

use bcrypt::{DEFAULT_COST, hash};
use chrono::{TimeZone, Utc};
use diesel::{OptionalExtension, QueryDsl, expression_methods::ExpressionMethods};
use diesel_async::RunQueryDsl;
use gumdrop::Options;
use lemmy_db_views::structs::SiteView;
use log::{LevelFilter, debug, error, info};
use serde::Deserialize;
use walkdir::WalkDir;

use lemmy_db_schema::{
    schema::{person, post},
    source::{
        community::Community,
        local_user::{LocalUser, LocalUserInsertForm},
        person::{Person, PersonInsertForm},
        post::{Post, PostInsertForm},
    },
    traits::{ApubActor, Crud},
    utils::{DbPool, build_db_pool, get_conn},
};

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

    #[options(help = "just generate users without inserting posts")]
    gen_users_only: bool,
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
    pub comments: Vec<RedditComment>,
}

#[derive(Debug, Deserialize)]
pub struct RedditComment {
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
    pub replies: Vec<RedditComment>, // Recursively nested replies
}

#[tokio::main]
async fn main() {
    let opts = CommandOptions::parse_args_default_or_exit();

    let mut builder = env_logger::Builder::new();

    if opts.verbose {
        builder.filter_level(LevelFilter::Debug);
    } else {
        builder.filter_level(LevelFilter::Info);
    }
    builder.init();

    env::var("LEMMY_INITIALIZE_WITH_DEFAULT_SETTINGS").expect("Please set LEMMY_INITIALIZE_WITH_DEFAULT_SETTINGS to anything in order to allow usage of the Lemmy API without a Lemmy config.");
    env::var("LEMMY_DATABASE_URL").expect(
        "Please set LEMMY_DATABASE_URL to allow usage of the Lemmy API without a Lemmy config.",
    );
    let new_user_password = env::var("NEW_USER_PASSWORD")
        .expect("Please set NEW_USER_PASSWORD to be the password set for newly created users.");

    info!("Connecting to DB");

    // Build the actual pool
    let actual_pool = build_db_pool()
        .await
        .expect("Failed to create Lemmy DB pool");
    // Wrap it as a DbPool for use in Lemmy functions
    let mut db_pool = DbPool::Pool(&actual_pool);

    if let Some(Command::Import(ref import_options)) = opts.command {
        let path = Path::new(&import_options.archive_path);

        let site_view = SiteView::read_local(&mut db_pool).await.unwrap().unwrap(); // Safe to assume the site is set up

        let new_user_password_hash = hash(new_user_password, DEFAULT_COST).unwrap();

        info!("Inserting archives inside {} into Lemmy DB", path.display());

        for entry in WalkDir::new(path) {
            // TODO: Increase speed with concurrency
            let entry = entry.as_ref().unwrap().path();

            if entry.is_file() {
                let file = File::open(entry).unwrap();
                let reader = BufReader::new(file);

                let post: RedditPost =
                    serde_json::from_reader(reader).expect("Unable to parse post:");

                // Post::create(&mut db_pool, form);
                let community_name = post
                    .permalink
                    .strip_prefix("/r/")
                    .and_then(|s| s.split('/').next())
                    .unwrap_or("");

                let community_name = format!("{community_name}Mirror");
                debug!("community_name: {community_name}");

                match Community::read_from_name(&mut db_pool, &community_name, false).await {
                    Ok(community_id) => {
                        match Person::read_from_name(&mut db_pool, &post.author, false).await {
                            Ok(user) => {
                                let user_id = match user {
                                    Some(user) => user.id,
                                    None => {
                                        info!(
                                            "User {} not found, creating a local user...",
                                            &post.author
                                        );

                                        let person_form = PersonInsertForm::new(
                                            post.author.clone(),
                                            site_view.site.public_key.clone(),
                                            site_view.site.instance_id,
                                        );

                                        let new_user = Person::create(&mut db_pool, &person_form)
                                            .await
                                            .unwrap();

                                        let local_user_form = LocalUserInsertForm::new(
                                            new_user.id,
                                            new_user_password_hash.clone(),
                                        );

                                        LocalUser::create(&mut db_pool, &local_user_form, vec![])
                                            .await
                                            .unwrap();

                                        new_user.id
                                    }
                                };

                                let conn = &mut get_conn(&mut db_pool).await.unwrap();

                                match post::table
                                    .filter(post::name.eq(post.title.clone()))
                                    .filter(post::body.eq(post.selftext.clone()))
                                    .first::<Post>(conn)
                                    .await
                                    .optional()
                                    .unwrap()
                                {
                                    Some(_) => {
                                        info!("Skipping duplicate post {}", post.title.clone())
                                    }
                                    None => {
                                        if !import_options.gen_users_only {
                                            let publish_date = Utc
                                                .timestamp_opt(post.created_utc.trunc() as i64, 0)
                                                .unwrap();

                                            let lemmy_post = PostInsertForm::builder()
                                                .name(post.title.clone())
                                                .creator_id(user_id)
                                                .community_id(community_id.unwrap().id)
                                                .nsfw(Some(post.over_18))
                                                .body(Some(post.selftext))
                                                .published(Some(publish_date))
                                                .build();

                                            // // In order to properly federate, each vote must come from an user. So we use all the users we have to vote, and then generate lots of dummys for the remaining votes.
                                            // let current_users =
                                            //     person::table.load::<Person>(conn).await.unwrap();

                                            info!("Inserting {} into DB", post.title);

                                            let _lemmy_post =
                                                Post::create(&mut db_pool, &lemmy_post)
                                                    .await
                                                    .unwrap();
                                        }
                                    }
                                };
                            }
                            Err(e) => {
                                error!("Lemmy user doesn't exist: {e}")
                            }
                        };
                    }
                    Err(e) => {
                        error!("Lemmy community doesn't exist: {e}")
                    }
                }
            }
        }
    }
}
