use std::{
    // TODO: Paralellize, reduce uneeded DB reads, batch writes
    cmp,
    collections::HashMap,
    env,
    fs::File,
    io::BufReader,
    path::{Path, PathBuf},
};

use bcrypt::{DEFAULT_COST, hash};
use chrono::{TimeZone, Utc};
use diesel::{OptionalExtension, QueryDsl, expression_methods::ExpressionMethods, insert_into};
use diesel_async::RunQueryDsl;
use futures::{StreamExt, stream};
use gumdrop::Options;
use lemmy_db_views::structs::SiteView;
use log::{LevelFilter, debug, error, info};
use serde::Deserialize;
use walkdir::WalkDir;

use lemmy_db_schema::{
    aggregates::{post_aggregates, structs::PostAggregates},
    newtypes::CommentId,
    schema::{
        comment_like, person, post,
        post_like::{self},
    },
    source::{
        comment::{Comment, CommentInsertForm, CommentLikeForm},
        community::Community,
        local_user::{LocalUser, LocalUserInsertForm},
        person::{Person, PersonInsertForm},
        post::{Post, PostInsertForm, PostLikeForm},
    },
    traits::{ApubActor, Crud},
    utils::{DbPool, build_db_pool, get_conn},
};
use rand::{Rng, distr::Alphanumeric};

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

    #[options(help = "force all posts to be not marked nsfw")]
    dont_mark_nsfw: bool,
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

impl RedditComment {
    fn count_recursive(&self) -> usize {
        1 + self
            .replies
            .iter()
            .map(|reply| reply.count_recursive())
            .sum::<usize>()
    }
}

impl RedditPost {
    fn count_recursive(&self) -> usize {
        self.comments
            .iter()
            .map(|reply| reply.count_recursive())
            .sum::<usize>()
    }
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

        let concurrency_limit = 15;

        let entries = WalkDir::new(path)
            .into_iter()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_type().is_file())
            .map(|e| e.into_path());

        stream::iter(entries).map(|path: PathBuf|{
            let owned_pool = actual_pool.clone();
            let site_view = site_view.clone();
            let new_user_password_hash = new_user_password_hash.clone();

            async move {
            let mut db_pool = DbPool::Pool(&owned_pool);

            let mut db_pool_for_conn = DbPool::Pool(&owned_pool);
            let mut conn = get_conn(&mut db_pool_for_conn).await.unwrap();

            if path.is_file() {

                let post: RedditPost = tokio::task::spawn_blocking(move || {
                    let file = File::open(path).unwrap();
                    let reader = BufReader::new(file);
                    serde_json::from_reader(reader)
                })
                .await
                .unwrap()
                .expect("Unable to parse post");

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
                        let user_id = get_or_create_user(
                            &post.author,
                            &site_view,
                            &mut db_pool,
                            &new_user_password_hash,
                        )
                        .await
                        .id;

                        let title_trunc: String = post.title.chars().take(200).collect();

                        let skip_dupe = match post::table
                            .filter(post::name.eq(title_trunc.clone()))
                            .filter(post::body.eq(post.selftext.clone()))
                            .first::<Post>(&mut conn)
                            .await
                            .optional()
                            .unwrap()
                        {
                            Some(existing_post) => {
                                let post_comment_count = PostAggregates::read(&mut db_pool, existing_post.id).await.unwrap().unwrap().comments;

                                let loaded_post_comments = post.count_recursive();
                                if loaded_post_comments == post_comment_count as usize {
                                    info!("Skipping duplicate post {} with {} comments", post.title.clone(), post.comments.len());
                                    true
                                } else {
                                    info!("Updating post {} ({} -> {} comments)", post.title.clone(), post_comment_count, loaded_post_comments);
                                    false
                                }
                            }
                            None => {
                                false
                            }
                        };

                        if !skip_dupe && !import_options.gen_users_only {
                            info!("Inserting post {} into DB", post.title);

                            let publish_date = Utc
                                .timestamp_opt(post.created_utc.trunc() as i64, 0)
                                .unwrap();

                            let nsfw = {
                                if import_options.dont_mark_nsfw {
                                    false
                                } else {
                                    post.over_18
                                }
                            };
                            let lemmy_post = PostInsertForm::builder()
                                .name(title_trunc)
                                .creator_id(user_id)
                                .community_id(community_id.unwrap().id)
                                .nsfw(Some(nsfw))
                                .body(Some(post.selftext))
                                .published(Some(publish_date))
                                .build();

                            // In order to properly federate, each vote must come from an user. So we use all the users we have to vote, and then generate lots of dummys for the remaining votes.
                            let mut voting_users =
                                person::table.load::<Person>(&mut conn).await.unwrap();

                            for _ in 0..post.score.saturating_sub(voting_users.len().try_into().unwrap()) {
                                let rand_string: String = rand::rng()
                                .sample_iter(&Alphanumeric)
                                .take(7)
                                .map(char::from)
                                .collect();

                                let username = format!("voteuser-{rand_string}");
                                debug!("Adding {username} as a voteuser");

                                let user = get_or_create_user(
                                    &username,
                                    &site_view,
                                    &mut db_pool,
                                    &new_user_password_hash,
                                ).await;

                                voting_users.push(user);
                            }

                            let lemmy_post_id =
                                Post::create(&mut db_pool, &lemmy_post).await.unwrap().id;

                            let mut voting_users_trunc = voting_users.clone();

                            voting_users_trunc.truncate(post.score.try_into().unwrap()); // Now we can just iterate through voting_users_trunc

                            let like_forms: Vec<_> = voting_users_trunc
                                .iter()
                                .map(|user| PostLikeForm {
                                    post_id: lemmy_post_id,
                                    person_id: user.id,
                                    score: 1,
                                })
                                .collect();

                            insert_into(post_like::table)
                                .values(&like_forms)
                                .execute(&mut conn)
                                .await
                                .unwrap();

                            // Insert comments

                            let mut flat_comments = Vec::new();
                            walk_comments(None, &post.comments, &mut flat_comments);

                            let mut reddit_lemmy_id: HashMap<String, CommentId> =
                                HashMap::new();
                            // Since walking the path will always insert parent comments first, we can insert the comment Reddit id and Lemmy id
                            // allowing us to use the reddit coment parent id to determine the correct parent to set a reply to.

                            for (parent_id, comment) in flat_comments {
                                debug!(
                                    "Inserting comment with Comment ID: {}, Parent ID: {:?}, Body: {}",
                                    comment.id, parent_id, comment.body
                                );
                                info!(
                                    "Inserting comment with score {} by '{}'",
                                    comment.score, comment.author
                                );

                                let author_id = get_or_create_user(
                                    &comment.author,
                                    &site_view,
                                    &mut db_pool,
                                    &new_user_password_hash,
                                )
                                .await
                                .id;

                                let publish_date = Utc
                                    .timestamp_opt(comment.created_utc.trunc() as i64, 0)
                                    .unwrap();

                                let lemmy_comment = CommentInsertForm::builder()
                                    .creator_id(author_id)
                                    .post_id(lemmy_post_id)
                                    .content(comment.body.clone())
                                    .published(Some(publish_date))
                                    .build();

                                let new_lemmy_comment_id = {
                                    if parent_id.is_none() {
                                        let new_lemmy_comment_id = Comment::create(
                                            &mut db_pool,
                                            &lemmy_comment,
                                            None,
                                        )
                                        .await
                                        .unwrap()
                                        .id;

                                        reddit_lemmy_id.insert(
                                            comment.id.clone(),
                                            new_lemmy_comment_id,
                                        );

                                        new_lemmy_comment_id
                                    } else {
                                        let lemmy_parent_id = reddit_lemmy_id
                                            .get(parent_id.unwrap())
                                            .unwrap(); // Unwrap since walk_comments should always run in order

                                        let parent_comment_path =
                                            Comment::read(&mut db_pool, *lemmy_parent_id)
                                                .await
                                                .unwrap()
                                                .unwrap()
                                                .path;

                                        let new_lemmy_comment_id = Comment::create(
                                            &mut db_pool,
                                            &lemmy_comment,
                                            Some(&parent_comment_path),
                                        )
                                        .await
                                        .unwrap()
                                        .id;

                                        reddit_lemmy_id.insert(
                                            comment.id.clone(),
                                            new_lemmy_comment_id,
                                        );

                                        new_lemmy_comment_id
                                    }
                                };

                                // Apply score to comment
                                // TODO: Make a function, reduce uneeded DB requests, just clean up in general

                                let score = comment.score;
                                let vote_count = score.unsigned_abs() as usize;

                                for _ in 0..vote_count.saturating_sub(voting_users.len()) {
                                    let rand_string: String = rand::rng()
                                    .sample_iter(&Alphanumeric)
                                    .take(7)
                                    .map(char::from)
                                    .collect();

                                    let username = format!("voteuser-{rand_string}");
                                    debug!("Adding {username} as a voteuser");

                                    let user = get_or_create_user(
                                        &username,
                                        &site_view,
                                        &mut db_pool,
                                        &new_user_password_hash,
                                    ).await;

                                    voting_users.push(user);
                                }

                                let mut voting_users_trunc = voting_users.clone();
                                voting_users_trunc.truncate(vote_count);

                                // Determine the score each vote should get
                                let score_value = if score > 0 { 1 } else { -1 };

                                let like_forms: Vec<_> = voting_users_trunc
                                    .iter()
                                    .map(|user| CommentLikeForm {
                                        comment_id: new_lemmy_comment_id,
                                        post_id: lemmy_post_id,
                                        person_id: user.id,
                                        score: score_value,
                                    })
                                    .collect();

                                insert_into(comment_like::table)
                                    .values(&like_forms)
                                    .execute(&mut conn)
                                    .await
                                    .unwrap();

                                reddit_lemmy_id.insert(comment.id.clone(), new_lemmy_comment_id);

                            }
                        }
                    }
                    Err(e) => {
                        error!("Lemmy community doesn't exist: {e}")
                    }}
                }
            }
        })
        .buffer_unordered(concurrency_limit)
        .collect::<Vec<_>>() // Collect to drive the stream to completion
        .await;
    }
}

async fn get_or_create_user(
    username: &String,
    site_view: &SiteView,
    db_pool: &mut DbPool<'_>,
    new_user_password_hash: &str,
) -> Person {
    let username = format!("{username}-mirror");
    let username_trunc: String = username.chars().take(20).collect();

    match Person::read_from_name(db_pool, &username_trunc, false).await {
        Ok(user) => match user {
            Some(user) => user,
            None => {
                info!(
                    "User {} not found, creating a local user...",
                    &username_trunc
                );

                let person_form = PersonInsertForm::new(
                    username_trunc.clone(),
                    site_view.site.public_key.clone(),
                    site_view.site.instance_id,
                );

                let new_user = Person::create(db_pool, &person_form).await.unwrap();

                let local_user_form =
                    LocalUserInsertForm::new(new_user.id, new_user_password_hash.to_owned());

                LocalUser::create(db_pool, &local_user_form, vec![])
                    .await
                    .unwrap();

                new_user
            }
        },
        Err(e) => panic!("Couldn't query db: {e}"),
    }
}

fn walk_comments<'a>(
    parent_id: Option<&'a str>,
    comments: &'a [RedditComment],
    out: &mut Vec<(Option<&'a str>, &'a RedditComment)>,
) {
    for comment in comments {
        out.push((parent_id, comment));
        walk_comments(Some(&comment.id), &comment.replies, out);
    }
}
