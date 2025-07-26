use std::{
    // TODO: Paralellize, reduce uneeded DB reads, batch writes
    collections::HashMap,
    env,
    path::{Path, PathBuf},
    sync::Arc,
    time::Instant,
};
use tokio::io::AsyncReadExt;

use bcrypt::{DEFAULT_COST, hash};
use chrono::{TimeZone, Utc};
use diesel::query_dsl::methods::FilterDsl;
use diesel::{ExpressionMethods, insert_into};
use diesel_async::{
    AsyncPgConnection, RunQueryDsl,
    pooled_connection::{AsyncDieselConnectionManager, deadpool::Pool},
};
use futures::{StreamExt, stream};
use gumdrop::Options;
use indicatif::ProgressBar;
use lemmy_db_views::structs::SiteView;
use log::{LevelFilter, debug, error, info};
use serde::Deserialize;
use tokio::fs::File;
use walkdir::WalkDir;

use lemmy_db_schema::source::post::PostUpdateForm;
use lemmy_db_schema::{
    aggregates::structs::{CommunityAggregates, PostAggregates},
    newtypes::{CommentId, PersonId, PostId},
    schema::{
        comment_like, person, post,
        post_like::{self},
    },
    source::{
        comment::{self, Comment, CommentInsertForm, CommentLikeForm},
        community::{self, Community},
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

    #[options(help = "load user list per post inserted")]
    load_users_every_post: bool,

    #[options(help = "only print progress bar")]
    only_progress: bool,

    #[options(help = "don't spawn tasks for imports")]
    no_tasks: Option<bool>,

    #[options(help = "what to override username with")]
    username_override: Option<String>,

    #[options(help = "skip posts or comments with a score below 1")]
    skip_low_score: Option<bool>,
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

        let entries: Vec<_> = WalkDir::new(path)
            .into_iter()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_type().is_file())
            .map(|e| e.into_path())
            .collect();

        let pb = ProgressBar::new(entries.len() as u64);

        let mut initial_conn = get_conn(&mut db_pool).await.unwrap();

        // TODO: load to HashMap?

        info!("Loading users from db...");
        let voting_users = person::table
            .load::<Person>(&mut initial_conn)
            .await
            .unwrap();

        info!("Loading existing posts from db...");
        let post_list_vec = post::table.load::<Post>(&mut initial_conn).await.unwrap();

        let post_list: HashMap<(String, String), PostId> = post_list_vec
            .into_iter()
            .map(|p| ((p.name, p.body.unwrap()), p.id))
            .collect();

        info!("Loading post metadata from db...");
        let post_aggregates_list_vec = lemmy_db_schema::schema::post_aggregates::table
            .load::<PostAggregates>(&mut initial_conn)
            .await
            .unwrap();

        let post_aggregates_list: HashMap<PostId, i64> = post_aggregates_list_vec
            .into_iter()
            .map(|pa| (pa.post_id, pa.comments))
            .collect();

        info!("Loading communities from db..."); // Don't put in HashMap since it's likely very short list
        let community_list = lemmy_db_schema::schema::community::table
            .load::<Community>(&mut initial_conn)
            .await
            .unwrap();

        drop(initial_conn);

        if !import_options.no_tasks.unwrap_or(false) {
            stream::iter(entries)
                .map(|path: PathBuf| {
                    let owned_pool = actual_pool.clone();
                    let site_view = site_view.clone();
                    let new_user_password_hash = new_user_password_hash.clone();
                    let mut voting_users = voting_users.clone();
                    let post_list = post_list.clone();
                    let community_list = community_list.clone();
                    let post_aggregates_list = post_aggregates_list.clone();
                    let pb = pb.clone();

                    async move {
                        process_post(
                            &path,
                            &community_list,
                            &post_list,
                            &post_aggregates_list,
                            import_options,
                            Some(&owned_pool),
                            &site_view,
                            &new_user_password_hash,
                            &mut voting_users,
                            &pb,
                        )
                        .await;
                    }
                })
                .buffer_unordered(concurrency_limit)
                .collect::<Vec<_>>() // Collect to drive the stream to completion
                .await;

            pb.finish();
        } else {
            let owned_pool = actual_pool.clone();
            let site_view = site_view.clone();
            let new_user_password_hash = new_user_password_hash.clone();
            let mut voting_users = voting_users.clone();
            let post_list = post_list.clone();
            let community_list = community_list.clone();
            let post_aggregates_list = post_aggregates_list.clone();
            let pb = pb.clone();

            for entry in entries {
                process_post(
                    &entry,
                    &community_list,
                    &post_list,
                    &post_aggregates_list,
                    import_options,
                    Some(&owned_pool),
                    &site_view,
                    &new_user_password_hash,
                    &mut voting_users,
                    &pb,
                )
                .await;
            }
        }
    }
}

async fn get_or_create_user(
    override_username: &Option<String>,
    username: &String,
    site_view: &SiteView,
    db_pool: &mut DbPool<'_>,
    new_user_password_hash: &str,
) -> Person {
    let username = override_username
        .clone()
        .unwrap_or(format!("{username}-mirror"));
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

async fn process_post(
    path: &std::path::Path,
    community_list: &[Community],
    post_list: &HashMap<(String, String), PostId>,
    post_aggregates_list: &HashMap<PostId, i64>,
    import_options: &ImportOptions,
    owned_pool: Option<&Pool<AsyncPgConnection>>,
    site_view: &SiteView,
    new_user_password_hash: &str,
    voting_users: &mut Vec<Person>,
    pb: &ProgressBar,
) {
    if path.is_file() {
        let mut file = File::open(&path).await.expect("Failed to open file");
        let mut contents = Vec::new();
        file.read_to_end(&mut contents)
            .await
            .expect("Failed to read file");

        let post: RedditPost = serde_json::from_slice(&contents).expect("Unable to parse post");

        let community_name = post
            .permalink
            .strip_prefix("/r/")
            .and_then(|s| s.split('/').next())
            .unwrap_or("");

        let community_name = format!("{community_name}Mirror");
        debug!("community_name: {community_name}");

        match community_list.iter().find(|p| p.name == community_name) {
            Some(community) => {
                let community_id = community.id;

                let title_trunc: String = post.title.chars().take(200).collect();

                let (post_id, skip_dupe) =
                    match post_list.get(&(title_trunc.clone(), post.selftext.clone())) {
                        Some(existing_post_id) => {
                            let post_comment_count =
                                post_aggregates_list.get(existing_post_id).unwrap();

                            let loaded_post_comments = post.count_recursive();
                            if loaded_post_comments == *post_comment_count as usize {
                                if !import_options.only_progress {
                                    info!(
                                        "Skipping duplicate post {} with {} comments",
                                        post.title.clone(),
                                        post.comments.len()
                                    );
                                }
                                (Some(existing_post_id), true)
                            } else {
                                if !import_options.only_progress {
                                    info!(
                                        "Updating post {} ({} -> {} comments)",
                                        post.title.clone(),
                                        post_comment_count,
                                        loaded_post_comments
                                    );
                                }
                                (Some(existing_post_id), false)
                            }
                        }
                        None => (None, false),
                    };

                let skip_dupe = {
                    if import_options.skip_low_score.unwrap_or(false) {
                        if post.score <= 0 { true } else { skip_dupe }
                    } else {
                        skip_dupe
                    }
                };

                if !skip_dupe && !import_options.gen_users_only {
                    let owned_pool = owned_pool.unwrap();

                    let mut db_pool = DbPool::Pool(owned_pool);

                    let mut db_pool_for_conn = DbPool::Pool(owned_pool);
                    let mut conn = get_conn(&mut db_pool_for_conn).await.unwrap();

                    let new_username = Some(format!(
                        "{} OP",
                        &import_options.username_override.clone().unwrap()
                    )); // this code sucks

                    let user_id = get_or_create_user(
                        {
                            if import_options.username_override.is_some() {
                                &new_username
                            } else {
                                &import_options.username_override
                            }
                        },
                        &post.author,
                        site_view,
                        &mut db_pool,
                        new_user_password_hash,
                    )
                    .await
                    .id;

                    if !import_options.only_progress {
                        info!("Inserting post {} into DB", post.title);
                    }

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
                        .community_id(community_id)
                        .nsfw(Some(nsfw))
                        .body(Some(post.selftext))
                        .published(Some(publish_date))
                        .build();

                    let voting_users = {
                        if import_options.load_users_every_post {
                            &mut person::table.load::<Person>(&mut conn).await.unwrap()
                        } else {
                            voting_users
                        }
                    };

                    // TODO: Don't remove entire post just to reinsert it

                    if let Some(post_id) = post_id {
                        info!("Updating existing post {}", post.title);

                        Post::update(
                            &mut db_pool,
                            *post_id,
                            &PostUpdateForm {
                                deleted: Some(true),
                                ..Default::default()
                            },
                        )
                        .await
                        .unwrap();
                    }

                    let lemmy_post_id = Post::create(&mut db_pool, &lemmy_post).await.unwrap().id;

                    for _ in 0..post
                        .score
                        .saturating_sub(voting_users.len().try_into().unwrap())
                    {
                        let rand_string: String = rand::rng()
                            .sample_iter(&Alphanumeric)
                            .take(7)
                            .map(char::from)
                            .collect();

                        let username = format!("voteuser-{rand_string}");
                        debug!("Adding {username} as a voteuser");

                        let user = get_or_create_user(
                            &None,
                            &username,
                            site_view,
                            &mut db_pool,
                            new_user_password_hash,
                        )
                        .await;

                        voting_users.push(user);
                    }

                    let mut voting_users_trunc = voting_users.clone();

                    voting_users_trunc.truncate(post.score.try_into().unwrap()); // Now we can just iterate through voting_users_trunc

                    let score_value = if post.score > 0 { 1 } else { -1 };

                    let like_forms: Vec<_> = voting_users_trunc
                        .iter()
                        .map(|user| PostLikeForm {
                            post_id: lemmy_post_id,
                            person_id: user.id,
                            score: score_value,
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

                    let mut reddit_lemmy_id: HashMap<String, CommentId> = HashMap::new();
                    // Since walking the path will always insert parent comments first, we can insert the comment Reddit id and Lemmy id
                    // allowing us to use the reddit coment parent id to determine the correct parent to set a reply to.

                    for (parent_id, comment) in flat_comments {
                        let skip_comment = {
                            import_options.skip_low_score.unwrap_or(false) && comment.score <= 1
                        };

                        if !skip_comment {
                            debug!(
                                "Inserting comment with Comment ID: {}, Parent ID: {:?}, Body: {}",
                                comment.id, parent_id, comment.body
                            );

                            if !import_options.only_progress {
                                info!(
                                    "Inserting comment with score {} by '{}' on post '{}'",
                                    comment.score, comment.author, post.title
                                );
                            }

                            let author_id = get_or_create_user(
                                {
                                    if post.author == comment.author {
                                        &new_username
                                    } else {
                                        &import_options.username_override
                                    }
                                },
                                &comment.author,
                                site_view,
                                &mut db_pool,
                                new_user_password_hash,
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
                                    let new_lemmy_comment_id =
                                        Comment::create(&mut db_pool, &lemmy_comment, None)
                                            .await
                                            .unwrap()
                                            .id;

                                    reddit_lemmy_id
                                        .insert(comment.id.clone(), new_lemmy_comment_id);

                                    Some(new_lemmy_comment_id)
                                } else {
                                    match reddit_lemmy_id.get(parent_id.unwrap()) {
                                        Some(lemmy_parent_id) => {
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

                                            reddit_lemmy_id
                                                .insert(comment.id.clone(), new_lemmy_comment_id);

                                            Some(new_lemmy_comment_id)
                                        }
                                        None => None,
                                    }
                                }
                            };

                            // Apply score to comment
                            // TODO: Make a function, reduce uneeded DB requests, just clean up in general

                            if let Some(new_lemmy_comment_id) = new_lemmy_comment_id {
                                let score = comment.score;
                                let vote_count = score.unsigned_abs() as usize;

                                for _ in 0..vote_count.saturating_sub(voting_users.len()) {
                                    let rand_string: String = rand::thread_rng()
                                        .sample_iter(&Alphanumeric)
                                        .take(7)
                                        .map(char::from)
                                        .collect();

                                    let username = format!("voteuser-{rand_string}");
                                    debug!("Adding {username} as a voteuser");

                                    let user = get_or_create_user(
                                        &None,
                                        &username,
                                        &site_view,
                                        &mut db_pool,
                                        &new_user_password_hash,
                                    )
                                    .await;

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

                                match insert_into(comment_like::table)
                                    .values(&like_forms)
                                    .execute(&mut conn)
                                    .await
                                {
                                    Ok(_) => {}
                                    Err(e) => {
                                        error!(
                                            "Failed to insert comment_likes with:\nvoting_users_trunc: {:?}\n error: {e}",
                                            voting_users_trunc
                                        )
                                    }
                                }

                                reddit_lemmy_id.insert(comment.id.clone(), new_lemmy_comment_id);
                            }
                        }
                    }
                }
            }
            None => {
                error!("Lemmy community doesn't exist: {community_name}")
            }
        }
    }
    if !import_options.only_progress {
        pb.inc(1);
    }
}
