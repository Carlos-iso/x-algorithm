use crate::filter::{FilterRequest, FilterTweets};
use crate::hydration::request_context;
use crate::models::{ClientCapability, RawCandidate, TweetId};
use crate::params::CountryList;
use crate::retweet;
use crate::rules::metrics::Rpc;
use crate::rules::SafetyLevel;
use crate::staging::recording::Recording;
use crate::staging::reference::tweetypie::{vf_label, Client, Label};
use crate::treatment;
use anyhow::Context;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;
use tokio::time::Instant;

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct Case {
    pub(crate) id: String,
    pub(crate) tags: Vec<String>,
    pub(crate) captured_at_unix: u64,
    pub(crate) build: String,
    pub(crate) viewer_id: u64,
    pub(crate) country_code: Option<String>,
    #[serde(default)]
    pub(crate) client: Client,
    pub(crate) client_capability: ClientCapability,
    pub(crate) country_lists: BTreeMap<CountryList, Vec<String>>,
    pub(crate) tweets: Vec<Expected>,
    pub(crate) recording: Recording,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct Expected {
    pub(crate) tweet_id: u64,
    pub(crate) tags: Vec<String>,
    pub(crate) tweetypie: Label,
    pub(crate) vf: Label,
    pub(crate) vf_rule: Option<String>,
}

pub(crate) struct Evaluated {
    pub(crate) tweet_id: u64,
    pub(crate) label: Label,
    pub(crate) rule: Option<&'static str>,
}

pub(crate) struct Fixtures {
    pub(crate) users: BTreeSet<u64>,
    pub(crate) tweets: BTreeSet<u64>,
}

impl Fixtures {
    pub(crate) fn load(path: &Path) -> anyhow::Result<Self> {
        #[derive(Deserialize)]
        struct File {
            users: Vec<Fixture>,
            tweets: Vec<Fixture>,
        }
        #[derive(Deserialize)]
        struct Fixture {
            id: u64,
        }
        let text = fs::read_to_string(path).with_context(|| path.display().to_string())?;
        let file: File = serde_json::from_str(&text).with_context(|| path.display().to_string())?;
        let ids = |fixtures: Vec<Fixture>| fixtures.into_iter().map(|f| f.id).collect();
        Ok(Self {
            users: ids(file.users),
            tweets: ids(file.tweets),
        })
    }

    pub(crate) fn strangers(&self, recording: &Recording) -> anyhow::Result<Vec<String>> {
        let users = recording
            .user_ids()?
            .into_iter()
            .filter(|id| !self.users.contains(id))
            .map(|id| format!("user {id}"));
        let tweets = recording
            .tweet_ids()
            .into_iter()
            .filter(|id| !self.tweets.contains(id))
            .map(|id| format!("tweet {id}"));
        Ok(users.chain(tweets).collect())
    }
}

pub(crate) async fn evaluate(
    filter_tweets: &FilterTweets,
    viewer_id: u64,
    country_code: Option<String>,
    client_capability: ClientCapability,
    tweet_ids: &[u64],
) -> Vec<Evaluated> {
    let candidates = tweet_ids
        .iter()
        .map(|&tweet_id| RawCandidate {
            tweet_id: TweetId(tweet_id),
            request_author_id: None,
        })
        .collect();
    let request = FilterRequest {
        viewer_id: Some(viewer_id),
        country_code,
        client_capability,
        safety_level: SafetyLevel::TimelineHomeHydration,
        candidates,
        rpc: Rpc::EvaluateTweets,
    };
    request_context(Instant::now(), None)
        .scope(retweet::evaluate_merging_sources(filter_tweets, request))
        .await
        .iter()
        .map(|outcome| Evaluated {
            tweet_id: outcome.tweet_id.0,
            label: vf_label(outcome),
            rule: treatment::decided_rows(outcome.evaluation.verdict())
                .next()
                .map(|(rule, _)| rule),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hydration::sources::InMemorySources;
    use crate::params::CountryLists;
    use crate::rules::RuleEngine;
    use crate::staging::reference::tweetypie::Class;
    use serde::de::DeserializeOwned;
    use std::path::PathBuf;
    use std::sync::Arc;

    const CORPUS: &str = "tests/replay_corpus";
    const SCREENED_DROPS: [&str; 9] = [
        "tweet_is_bounced",
        "author_block_viewer",
        "author_is_protected",
        "author_is_suspended",
        "exclusive_tweet",
        "nsfw_viewer_is_underage",
        "nsfw_viewer_has_no_stated_age",
        "nsfw_logged_out",
        "premium_tweet",
    ];

    fn renders_alike(vf: &Label, tweetypie: &Label) -> bool {
        vf == tweetypie
            || (vf.0 == <&str>::from(Class::BareDrop)
                && tweetypie.0 == <&str>::from(Class::Drop)
                && !SCREENED_DROPS.contains(&tweetypie.1.as_str()))
    }

    #[derive(Deserialize)]
    struct KnownDivergence {
        case: String,
        tweet_id: u64,
        vf: Label,
        reason: String,
    }

    fn corpus_dir() -> PathBuf {
        let cargo = Path::new(env!("CARGO_MANIFEST_DIR")).join(CORPUS);
        if cargo.exists() {
            cargo
        } else {
            Path::new("crates/x-product/xai-visibility-filtering-service").join(CORPUS)
        }
    }

    fn read<T: DeserializeOwned>(path: &Path) -> T {
        serde_json::from_str(&fs::read_to_string(path).unwrap())
            .unwrap_or_else(|e| panic!("{}: {e}", path.display()))
    }

    fn cases(dir: &Path) -> Vec<Case> {
        let cases = dir.join("cases");
        let mut paths: Vec<PathBuf> = fs::read_dir(&cases)
            .unwrap_or_else(|e| panic!("{}: {e}", cases.display()))
            .map(|entry| entry.unwrap().path())
            .collect();
        paths.sort();
        assert!(!paths.is_empty(), "{} holds no case", cases.display());
        paths.iter().map(|path| read(path)).collect()
    }

    #[tokio::test]
    async fn every_case_replays_to_tweetypies_answer() {
        let dir = corpus_dir();
        let divergences: Vec<KnownDivergence> = read(&dir.join("known_divergences.json"));
        let shared: Recording = read(&dir.join("shared.json"));
        let mut failures = Vec::new();
        let mut replayed = Vec::new();
        for mut case in cases(&dir) {
            replayed.extend(case.tweets.iter().map(|t| (case.id.clone(), t.tweet_id)));
            case.recording.fill_from(&shared);
            let sources = Arc::new(case.recording.sources(case.viewer_id));
            let country_lists = CountryLists::from_codes(&case.country_lists)
                .unwrap_or_else(|e| panic!("{}: {e}", case.id));
            let filter_tweets = FilterTweets::new(
                Arc::<InMemorySources>::clone(&sources),
                RuleEngine::with_country_lists(Arc::new(country_lists)),
            );
            let tweet_ids: Vec<u64> = case.tweets.iter().map(|t| t.tweet_id).collect();
            let evaluated = evaluate(
                &filter_tweets,
                case.viewer_id,
                case.country_code,
                case.client_capability,
                &tweet_ids,
            )
            .await;
            let misses = case.recording.misses(&sources);
            if !misses.is_empty() {
                failures.push(format!(
                    "{}: the recording lacks {misses:?}; re-capture it with --test-users from fixtures.json",
                    case.id
                ));
                continue;
            }
            for expected in &case.tweets {
                let Some(vf) = evaluated.iter().find(|vf| vf.tweet_id == expected.tweet_id) else {
                    failures.push(format!("{}/{}: no outcome", case.id, expected.tweet_id));
                    continue;
                };
                let known = divergences
                    .iter()
                    .find(|d| d.case == case.id && d.tweet_id == expected.tweet_id);
                let key = format!("{}/{}", case.id, expected.tweet_id);
                match (renders_alike(&vf.label, &expected.tweetypie), known) {
                    (true, None) => {}
                    (false, Some(known)) if vf.label == known.vf => {}
                    (false, Some(known)) => failures.push(format!(
                        "{key}: vf {:?} by {:?}, its known divergence pins {:?} ({})",
                        vf.label, vf.rule, known.vf, known.reason
                    )),
                    (false, None) => failures.push(format!(
                        "{key} [{}; {}]: vf {:?} by {:?}, tweetypie {:?} (vf at capture {:?} by {:?})",
                        case.tags.join(","),
                        expected.tags.join(","),
                        vf.label,
                        vf.rule,
                        expected.tweetypie,
                        expected.vf,
                        expected.vf_rule,
                    )),
                    (true, Some(known)) => failures.push(format!(
                        "{key}: now matches tweetypie; remove its known divergence ({})",
                        known.reason
                    )),
                }
            }
        }
        failures.extend(
            divergences
                .iter()
                .filter(|d| !replayed.contains(&(d.case.clone(), d.tweet_id)))
                .map(|d| {
                    format!(
                        "{}/{}: known divergence names no replayed tweet",
                        d.case, d.tweet_id
                    )
                }),
        );
        assert!(failures.is_empty(), "\n{}", failures.join("\n"));
    }

    #[test]
    fn every_user_and_tweet_the_corpus_names_is_a_fixture() {
        let dir = corpus_dir();
        let fixtures = Fixtures::load(&dir.join("fixtures.json")).unwrap();
        let shared: Recording = read(&dir.join("shared.json"));
        let named = |file: &str, recording: &Recording| -> Vec<String> {
            fixtures
                .strangers(recording)
                .unwrap_or_else(|e| panic!("{file}: {e:#}"))
                .into_iter()
                .map(|stranger| format!("{file}: {stranger}"))
                .collect()
        };
        let mut strangers = named("shared.json", &shared);
        for case in cases(&dir) {
            strangers.extend(named(&case.id, &case.recording));
            if !fixtures.users.contains(&case.viewer_id) {
                strangers.push(format!("{}: user {}", case.id, case.viewer_id));
            }
            strangers.extend(
                case.tweets
                    .iter()
                    .filter(|tweet| !fixtures.tweets.contains(&tweet.tweet_id))
                    .map(|tweet| format!("{}: tweet {}", case.id, tweet.tweet_id)),
            );
        }
        assert!(strangers.is_empty(), "not in fixtures.json: {strangers:?}");
    }
}
