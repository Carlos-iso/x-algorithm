use crate::clients::socialgraph_client::EdgeQuery;
use crate::hydration::batch::{Hydrated, RawHydrationBatch};
use crate::hydration::sources::Observer;
use crate::hydration::tweet_source::decode_tweet;
use anyhow::Context;
use prost::Message;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, Mutex, PoisonError};
use std::{fmt, iter};
use xai_core_entities::entities::{
    ConversationControl, GizmoduckUser, GizmoduckUserResult, PureCoreData,
};
use xai_core_entities::gizmoduck_client::QueryFields;
use xai_visibility_filtering_proto as vf_pb;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Answer<V> {
    Found(V),
    Partial(V),
    NotFound,
    Failed,
}

impl<V: Clone> Answer<V> {
    fn of<E>(result: Option<&Result<Option<V>, E>>) -> Self {
        match result {
            Some(Ok(Some(value))) => Self::Found(value.clone()),
            Some(Ok(None)) => Self::NotFound,
            Some(Err(_)) | None => Self::Failed,
        }
    }

    fn landed(hydrated: Option<&Hydrated<V>>) -> Self {
        match hydrated {
            Some(Hydrated::Found(value)) => Self::Found(value.clone()),
            Some(Hydrated::Partial(value)) => Self::Partial(value.clone()),
            Some(Hydrated::NotFound) => Self::NotFound,
            Some(Hydrated::Failed(_)) | None => Self::Failed,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Bytes(Vec<u8>);

impl fmt::Display for Bytes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.iter().try_for_each(|byte| write!(f, "{byte:02x}"))
    }
}

impl Serialize for Bytes {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for Bytes {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let hex = String::deserialize(deserializer)?;
        (0..hex.len())
            .step_by(2)
            .map(|at| {
                hex.get(at..at + 2)
                    .and_then(|pair| u8::from_str_radix(pair, 16).ok())
            })
            .collect::<Option<Vec<u8>>>()
            .map(Self)
            .ok_or_else(|| serde::de::Error::custom("odd-length or non-hex bytes"))
    }
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub(crate) struct Recording {
    pub(crate) pure_cores: BTreeMap<u64, Answer<PureCoreData>>,
    pub(crate) tweets: BTreeMap<u64, Answer<Bytes>>,
    pub(crate) conversation_controls: BTreeMap<u64, Answer<ConversationControl>>,
    pub(crate) safety_labels: BTreeMap<u64, Answer<Bytes>>,
    pub(crate) viewers: BTreeMap<u64, Answer<GizmoduckUser>>,
    pub(crate) viewer_fields: Vec<QueryFields>,
    pub(crate) users: BTreeMap<u64, Answer<GizmoduckUserResult>>,
    pub(crate) user_fields: Vec<QueryFields>,
    pub(crate) edges: BTreeMap<String, Answer<bool>>,
    pub(crate) viewer_countries: BTreeMap<u64, Answer<String>>,
    pub(crate) second_degree: BTreeMap<u64, Answer<bool>>,
    pub(crate) trusted_friends: BTreeMap<u64, Answer<bool>>,
}

impl Recording {
    pub(crate) fn unsuccessful(&self) -> Vec<String> {
        fn named<'a, K: fmt::Display + 'a, V: 'a>(
            name: &str,
            answers: impl IntoIterator<Item = (&'a K, &'a Answer<V>)>,
        ) -> Vec<String> {
            answers
                .into_iter()
                .filter(|(_, answer)| matches!(answer, Answer::Failed | Answer::Partial(_)))
                .map(|(key, _)| format!("{name}/{key}"))
                .collect()
        }
        let Self {
            pure_cores,
            tweets,
            conversation_controls,
            safety_labels,
            viewers,
            viewer_fields: _,
            users,
            user_fields: _,
            edges,
            viewer_countries,
            second_degree,
            trusted_friends,
        } = self;
        [
            named("pure_cores", pure_cores),
            named("tweets", tweets),
            named("conversation_controls", conversation_controls),
            named("safety_labels", safety_labels),
            named("viewers", viewers),
            named("users", users),
            named(
                "edges",
                edges
                    .iter()
                    .filter(|(_, answer)| **answer != Answer::Partial(false)),
            ),
            named("viewer_countries", viewer_countries),
            named("second_degree", second_degree),
            named("trusted_friends", trusted_friends),
        ]
        .concat()
    }

    pub(crate) fn user_ids(&self) -> anyhow::Result<BTreeSet<u64>> {
        fn decoded<T>(
            name: &str,
            answers: &BTreeMap<u64, Answer<Bytes>>,
            decode: impl Fn(&[u8]) -> anyhow::Result<T>,
        ) -> anyhow::Result<Vec<T>> {
            answers
                .iter()
                .filter_map(|(key, answer)| match answer {
                    Answer::Found(Bytes(bytes)) | Answer::Partial(Bytes(bytes)) => {
                        Some(decode(bytes).with_context(|| format!("{name}/{key}")))
                    }
                    Answer::NotFound | Answer::Failed => None,
                })
                .collect()
        }
        let Self {
            pure_cores,
            tweets,
            conversation_controls,
            safety_labels,
            viewers,
            viewer_fields: _,
            users,
            user_fields: _,
            edges,
            viewer_countries,
            second_degree,
            trusted_friends: _,
        } = self;
        let authors = answered(pure_cores)
            .flat_map(|core| {
                [
                    Some(core.author_id),
                    core.source_user_id,
                    core.in_reply_to_user_id,
                ]
            })
            .flatten();
        let controls = answered(conversation_controls).flat_map(|control| {
            iter::once(control.conversation_tweet_author_id)
                .chain(control.invited_user_ids.iter().copied())
        });
        let destinations = edges
            .keys()
            .filter_map(|key| key.rsplit('/').next()?.parse().ok());
        let exclusive_authors = decoded("tweets", tweets, decode_tweet)?
            .into_iter()
            .flatten()
            .filter_map(|tweet| tweet.exclusive_conversation_author_id);
        let applicable_users = decoded("safety_labels", safety_labels, |bytes| {
            let labels = vf_pb::SafetyLabelMap::decode(bytes)?.labels;
            anyhow::ensure!(
                !labels.values().any(|label| matches!(
                    label.safety_label_source,
                    Some(vf_pb::safety_label::SafetyLabelSource::ToolAction(_))
                )),
                "a tool-action label names an employee"
            );
            labels
                .into_values()
                .flat_map(|label| label.applicable_users)
                .map(|user| Ok(u64::try_from(user)?))
                .collect::<anyhow::Result<Vec<u64>>>()
        })?
        .into_iter()
        .flatten();
        Ok(viewers
            .keys()
            .chain(users.keys())
            .chain(viewer_countries.keys())
            .chain(second_degree.keys())
            .copied()
            .chain(authors)
            .chain(controls)
            .chain(destinations)
            .chain(exclusive_authors)
            .chain(applicable_users)
            .collect())
    }

    pub(crate) fn tweet_ids(&self) -> BTreeSet<u64> {
        let Self {
            pure_cores,
            tweets,
            conversation_controls,
            safety_labels,
            viewers: _,
            viewer_fields: _,
            users: _,
            user_fields: _,
            edges: _,
            viewer_countries: _,
            second_degree: _,
            trusted_friends: _,
        } = self;
        let linked = answered(pure_cores)
            .flat_map(|core| {
                [
                    core.source_tweet_id,
                    core.in_reply_to_tweet_id,
                    core.conversation_id,
                ]
            })
            .flatten();
        pure_cores
            .keys()
            .chain(tweets.keys())
            .chain(conversation_controls.keys())
            .chain(safety_labels.keys())
            .copied()
            .chain(linked)
            .collect()
    }
}

fn answered<V>(answers: &BTreeMap<u64, Answer<V>>) -> impl Iterator<Item = &V> {
    answers.values().filter_map(|answer| match answer {
        Answer::Found(value) | Answer::Partial(value) => Some(value),
        Answer::NotFound | Answer::Failed => None,
    })
}

#[cfg(test)]
impl Recording {
    pub(crate) fn fill_from(&mut self, shared: &Self) {
        fn fill<K: Ord + Clone, V: Clone>(own: &mut BTreeMap<K, V>, shared: &BTreeMap<K, V>) {
            for (key, value) in shared {
                own.entry(key.clone()).or_insert_with(|| value.clone());
            }
        }
        fill(&mut self.pure_cores, &shared.pure_cores);
        fill(&mut self.tweets, &shared.tweets);
        fill(
            &mut self.conversation_controls,
            &shared.conversation_controls,
        );
        fill(&mut self.safety_labels, &shared.safety_labels);
        fill(&mut self.users, &shared.users);
        add_fields(&mut self.user_fields, &shared.user_fields);
    }
}

fn edge_key(query: &EdgeQuery, destination: u64) -> String {
    format!(
        "{}/{:?}/{destination}",
        <&str>::from(query.graph),
        query.direction
    )
}

fn add_fields(recorded: &mut Vec<QueryFields>, fields: &[QueryFields]) {
    for field in fields {
        if !recorded.contains(field) {
            recorded.push(*field);
        }
    }
}

#[derive(Default)]
pub(crate) struct Recorder(Mutex<Recording>);

impl Recorder {
    pub(crate) fn take(&self) -> Recording {
        std::mem::take(&mut self.lock())
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Recording> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl Observer for Arc<Recorder> {
    fn pure_cores<E>(&self, ids: &[u64], cores: &HashMap<u64, Result<Option<PureCoreData>, E>>) {
        let mut recording = self.lock();
        for &id in ids {
            recording.pure_cores.insert(id, Answer::of(cores.get(&id)));
        }
    }

    fn tweets<B: AsRef<[u8]>, E>(&self, ids: &[u64], values: &HashMap<u64, Result<B, E>>) {
        let mut recording = self.lock();
        for &id in ids {
            let answer = match values.get(&id) {
                Some(Ok(bytes)) => Answer::Found(Bytes(bytes.as_ref().to_vec())),
                Some(Err(_)) | None => Answer::Failed,
            };
            recording.tweets.insert(id, answer);
        }
    }

    fn conversation_controls<E>(
        &self,
        ids: &[u64],
        controls: &HashMap<u64, Result<Option<ConversationControl>, E>>,
    ) {
        let mut recording = self.lock();
        for &id in ids {
            recording
                .conversation_controls
                .insert(id, Answer::of(controls.get(&id)));
        }
    }

    fn safety_labels<E>(
        &self,
        ids: &[u64],
        labels: &HashMap<u64, Result<Arc<vf_pb::SafetyLabelMap>, E>>,
    ) {
        let mut recording = self.lock();
        for &id in ids {
            let answer = match labels.get(&id) {
                Some(Ok(map)) => Answer::Found(Bytes(map.encode_to_vec())),
                Some(Err(_)) | None => Answer::Failed,
            };
            recording.safety_labels.insert(id, answer);
        }
    }

    fn viewer<E>(
        &self,
        viewer_id: u64,
        fields: &[QueryFields],
        viewer: &Result<Option<GizmoduckUser>, E>,
    ) {
        let mut recording = self.lock();
        add_fields(&mut recording.viewer_fields, fields);
        recording
            .viewers
            .insert(viewer_id, Answer::of(Some(viewer)));
    }

    fn users<E>(
        &self,
        ids: &[u64],
        fields: &[QueryFields],
        users: &HashMap<u64, Result<Option<GizmoduckUserResult>, E>>,
    ) {
        let mut recording = self.lock();
        add_fields(&mut recording.user_fields, fields);
        for &id in ids {
            recording.users.insert(id, Answer::of(users.get(&id)));
        }
    }

    fn edges(&self, queries: &[EdgeQuery], batches: &[RawHydrationBatch<bool>]) {
        let mut recording = self.lock();
        for (query, batch) in queries.iter().zip(batches) {
            for &destination in &query.destination_ids {
                recording.edges.insert(
                    edge_key(query, destination),
                    Answer::landed(batch.hydrated(&destination)),
                );
            }
        }
    }

    fn viewer_country(&self, viewer_id: u64, batch: &RawHydrationBatch<Arc<str>>) {
        let answer = match Answer::landed(batch.hydrated(&viewer_id)) {
            Answer::Found(country) => Answer::Found(country.to_string()),
            Answer::Partial(country) => Answer::Partial(country.to_string()),
            Answer::NotFound => Answer::NotFound,
            Answer::Failed => Answer::Failed,
        };
        self.lock().viewer_countries.insert(viewer_id, answer);
    }

    fn second_degree(&self, root_author_ids: &[u64], batch: &RawHydrationBatch<bool>) {
        let mut recording = self.lock();
        for &root in root_author_ids {
            recording
                .second_degree
                .insert(root, Answer::landed(batch.hydrated(&root)));
        }
    }

    fn trusted_friends(&self, list_ids: &[u64], batch: &RawHydrationBatch<bool>) {
        let mut recording = self.lock();
        for &list in list_ids {
            recording
                .trusted_friends
                .insert(list, Answer::landed(batch.hydrated(&list)));
        }
    }
}

#[cfg(test)]
mod replay {
    use super::*;
    use crate::clients::socialgraph_client::{EdgeDirection, Graph};
    use crate::hydration::plan::Source;
    use crate::hydration::sources::InMemorySources;
    use strum::VariantArray;

    fn unkept(name: &str, key: impl fmt::Display) -> ! {
        panic!("{name}/{key}: a failed or partial answer, which capture never keeps")
    }

    fn load<V>(
        sources: InMemorySources,
        name: &str,
        answers: &BTreeMap<u64, Answer<V>>,
        found: impl Fn(InMemorySources, u64, &V) -> InMemorySources,
    ) -> InMemorySources {
        answers
            .iter()
            .fold(sources, |sources, (&key, answer)| match answer {
                Answer::Found(value) => found(sources, key, value),
                Answer::NotFound => sources,
                Answer::Failed | Answer::Partial(_) => unkept(name, key),
            })
    }

    fn parse_edge_key(key: &str) -> (Graph, EdgeDirection, u64) {
        let (graph, rest) = key.split_once('/').unwrap_or_default();
        let (direction, destination) = rest.split_once('/').unwrap_or_default();
        let graph = Graph::VARIANTS.iter().find(|g| <&str>::from(**g) == graph);
        let direction = EdgeDirection::VARIANTS
            .iter()
            .find(|d| format!("{d:?}") == direction);
        match (graph, direction, destination.parse()) {
            (Some(&graph), Some(&direction), Ok(destination)) => (graph, direction, destination),
            _ => panic!("edges/{key}: not a graph/direction/destination key"),
        }
    }

    fn absent<V>(
        name: &str,
        recorded: &BTreeMap<u64, Answer<V>>,
        asked: Vec<Vec<u64>>,
    ) -> Vec<String> {
        asked
            .into_iter()
            .flatten()
            .filter(|key| !recorded.contains_key(key))
            .map(|key| format!("{name}/{key}"))
            .collect()
    }

    fn absent_fields(
        name: &str,
        recorded: &[QueryFields],
        asked: Vec<Vec<QueryFields>>,
    ) -> Vec<String> {
        let mut missing = vec![];
        for fields in asked {
            add_fields(&mut missing, &fields);
        }
        missing.retain(|field| !recorded.contains(field));
        if missing.is_empty() {
            vec![]
        } else {
            vec![format!("{name} fields {missing:?}")]
        }
    }

    impl Recording {
        pub(crate) fn sources(&self, viewer_id: u64) -> InMemorySources {
            let sources = load(
                InMemorySources::default(),
                "pure_cores",
                &self.pure_cores,
                |sources, id, core| sources.pure_core(id, core.clone()),
            );
            let sources = load(
                sources,
                "tweets",
                &self.tweets,
                |sources, id, Bytes(bytes)| match decode_tweet(bytes) {
                    Ok(Some(tweet)) => sources.tweet_features(id, tweet),
                    Ok(None) => sources,
                    Err(_) => sources.fail_key(Source::TesTweet, id),
                },
            );
            let sources = load(
                sources,
                "conversation_controls",
                &self.conversation_controls,
                |sources, id, control| sources.control(id, control.clone()),
            );
            let sources = load(
                sources,
                "safety_labels",
                &self.safety_labels,
                |sources, id, Bytes(bytes)| match vf_pb::SafetyLabelMap::decode(bytes.as_slice()) {
                    Ok(labels) => sources.labels(id, labels),
                    Err(_) => sources.fail_key(Source::SafetyLabels, id),
                },
            );
            let sources = load(sources, "viewers", &self.viewers, |sources, id, user| {
                sources.viewer(id, user.clone())
            });
            let sources = load(sources, "users", &self.users, |sources, id, user| {
                sources.user(id, user.clone())
            });
            let sources = load(
                sources,
                "viewer_countries",
                &self.viewer_countries,
                |sources, id, country| sources.country(id, country),
            );
            let sources = self
                .second_degree
                .iter()
                .fold(sources, |sources, (&root, answer)| match answer {
                    Answer::Found(true) => sources.second_degree_path(root, viewer_id),
                    Answer::Found(false) => sources,
                    Answer::NotFound => {
                        panic!("second_degree/{root}: InMemorySources cannot answer not found")
                    }
                    Answer::Failed | Answer::Partial(_) => unkept("second_degree", root),
                });
            let sources = self
                .trusted_friends
                .iter()
                .fold(sources, |sources, (&list, answer)| match answer {
                    Answer::Found(true) => sources.trusted_friend(list, viewer_id),
                    Answer::Found(false) => sources,
                    Answer::NotFound => {
                        panic!("trusted_friends/{list}: InMemorySources cannot answer not found")
                    }
                    Answer::Failed | Answer::Partial(_) => unkept("trusted_friends", list),
                });
            self.edges.iter().fold(sources, |sources, (key, answer)| {
                let (graph, direction, destination) = parse_edge_key(key);
                match (answer, direction) {
                    (Answer::Found(true), EdgeDirection::Forward) => {
                        sources.edge(graph, viewer_id, destination)
                    }
                    (Answer::Found(true), EdgeDirection::Reverse) => {
                        sources.edge(graph, destination, viewer_id)
                    }
                    (Answer::Found(false) | Answer::Partial(false), _) => sources,
                    (Answer::NotFound, _) => {
                        panic!("edges/{key}: InMemorySources cannot answer not found")
                    }
                    (Answer::Failed | Answer::Partial(true), _) => unkept("edges", key),
                }
            })
        }

        pub(crate) fn misses(&self, sources: &InMemorySources) -> Vec<String> {
            let mut misses: Vec<String> = Source::VARIANTS
                .iter()
                .flat_map(|&source| match source {
                    Source::TesPureCore => {
                        absent("pure_cores", &self.pure_cores, sources.keys(source))
                    }
                    Source::TesTweet => absent("tweets", &self.tweets, sources.keys(source)),
                    Source::TesConversationControl => absent(
                        "conversation_controls",
                        &self.conversation_controls,
                        sources.keys(source),
                    ),
                    Source::SafetyLabels => {
                        absent("safety_labels", &self.safety_labels, sources.keys(source))
                    }
                    Source::GizmoduckViewer => [
                        absent("viewers", &self.viewers, sources.keys(source)),
                        absent_fields("viewer", &self.viewer_fields, sources.fields(source)),
                    ]
                    .concat(),
                    Source::GizmoduckAuthor => [
                        absent("users", &self.users, sources.keys(source)),
                        absent_fields("users", &self.user_fields, sources.fields(source)),
                    ]
                    .concat(),
                    Source::Flock => sources
                        .selects()
                        .iter()
                        .flatten()
                        .flat_map(|query| {
                            query
                                .destination_ids
                                .iter()
                                .map(move |&destination| edge_key(query, destination))
                        })
                        .filter(|key| !self.edges.contains_key(key))
                        .map(|key| format!("edges/{key}"))
                        .collect(),
                    Source::ViewerCountry => absent(
                        "viewer_countries",
                        &self.viewer_countries,
                        sources.keys(source),
                    ),
                    Source::Wingman => {
                        absent("second_degree", &self.second_degree, sources.keys(source))
                    }
                    Source::CommunityModeration
                    | Source::CommunityModerator
                    | Source::ArticleLifecycle => sources
                        .keys(source)
                        .into_iter()
                        .flatten()
                        .map(|key| format!("{source:?}/{key}"))
                        .collect(),
                    Source::TrustedFriends => absent(
                        "trusted_friends",
                        &self.trusted_friends,
                        sources.keys(source),
                    ),
                })
                .collect();
            misses.sort();
            misses.dedup();
            misses
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clients::socialgraph_client::{EdgeDirection, Graph};
    use crate::filter::{FilterRequest, FilterTweets};
    use crate::hydration::batch::HydrationBatch;
    use crate::hydration::sources::{InMemorySources, Sources};
    use crate::models::{ClientCapability, RawCandidate, TweetId, ViewerProfile};
    use crate::rules::metrics::Rpc;
    use crate::rules::{RuleEngine, SafetyLevel};

    #[tokio::test]
    async fn a_call_the_recording_lacks_is_named() {
        let recording = Recording {
            pure_cores: BTreeMap::from([(
                1,
                Answer::Found(PureCoreData {
                    author_id: 10,
                    ..Default::default()
                }),
            )]),
            ..Default::default()
        };
        let sources = Arc::new(recording.sources(50));
        FilterTweets::new(
            Arc::<InMemorySources>::clone(&sources),
            RuleEngine::for_tests(),
        )
        .run(FilterRequest {
            viewer_id: Some(50),
            country_code: None,
            client_capability: ClientCapability::default(),
            safety_level: SafetyLevel::TimelineHomeHydration,
            candidates: vec![RawCandidate {
                tweet_id: TweetId(1),
                request_author_id: None,
            }],
            rpc: Rpc::FilterTweets,
        })
        .await;

        assert!(recording.misses(&sources).contains(&"tweets/1".to_string()));
    }

    #[tokio::test]
    async fn what_the_recorder_observes_replays_as_observed() {
        const VIEWER: u64 = 50;
        let paths = [(20, true), (21, false)];
        let lists = [(7, true), (8, false)];
        let edges = [
            (EdgeDirection::Forward, 30, Hydrated::Found(true), true),
            (EdgeDirection::Forward, 31, Hydrated::Found(false), false),
            (EdgeDirection::Reverse, 32, Hydrated::Found(true), true),
            (EdgeDirection::Reverse, 33, Hydrated::Found(false), false),
            (EdgeDirection::Forward, 34, Hydrated::Partial(false), false),
        ];
        let query = |direction, destination| EdgeQuery {
            graph: Graph::Follows,
            direction,
            destination_ids: vec![destination],
        };
        let recorder = Arc::new(Recorder::default());
        recorder.pure_cores(&[1], &HashMap::from([(1, Ok::<_, ()>(None))]));
        recorder.conversation_controls(&[1], &HashMap::from([(1, Ok::<_, ()>(None))]));
        recorder.users(&[10], &[], &HashMap::from([(10, Ok::<_, ()>(None))]));
        recorder.viewer(VIEWER, &[], &Ok::<_, ()>(None));
        recorder.viewer_country(
            VIEWER,
            &HydrationBatch::from_results(
                [VIEWER],
                HashMap::from([(VIEWER, Ok::<_, ()>(Some(Arc::from("us"))))]),
            ),
        );
        recorder.second_degree(
            &paths.map(|(root, _)| root),
            &HydrationBatch::from_results(
                paths.map(|(root, _)| root),
                HashMap::from(paths.map(|(root, path)| (root, Ok::<_, ()>(Some(path))))),
            ),
        );
        recorder.trusted_friends(
            &lists.map(|(list, _)| list),
            &HydrationBatch::from_results(
                lists.map(|(list, _)| list),
                HashMap::from(lists.map(|(list, holds)| (list, Ok::<_, ()>(Some(holds))))),
            ),
        );
        for (direction, destination, answer, _) in &edges {
            recorder.edges(
                &[query(*direction, *destination)],
                &[HydrationBatch::from_hydrated(HashMap::from([(
                    *destination,
                    answer.clone(),
                )]))],
            );
        }
        let recording = recorder.take();
        let sources = recording.sources(VIEWER);

        assert_eq!(recording.unsuccessful(), Vec::<String>::new());
        assert!(matches!(
            sources.pure_cores(&[1]).await.hydrated(&1),
            Some(Hydrated::NotFound)
        ));
        assert!(matches!(
            sources.conversation_controls(&[1]).await.hydrated(&1),
            Some(Hydrated::NotFound)
        ));
        assert!(matches!(
            sources.users(&[10], &[]).await.hydrated(&10),
            Some(Hydrated::NotFound)
        ));
        assert!(matches!(
            Sources::viewer(&sources, VIEWER, &[]).await.hydrated(&VIEWER),
            Some(Hydrated::Found(viewer)) if viewer.profile == ViewerProfile::default()
        ));
        assert_eq!(
            sources.viewer_country(VIEWER).await.hydrated(&VIEWER),
            Some(&Hydrated::Found(Arc::from("us")))
        );
        for (root, path) in paths {
            assert_eq!(
                sources.second_degree(VIEWER, &[root]).await.hydrated(&root),
                Some(&Hydrated::Found(path)),
                "second_degree/{root}"
            );
        }
        for (list, holds) in lists {
            assert_eq!(
                sources
                    .trusted_friends(VIEWER, &[list])
                    .await
                    .hydrated(&list),
                Some(&Hydrated::Found(holds)),
                "trusted_friends/{list}"
            );
        }
        for (direction, destination, _, holds) in edges {
            let answers = sources
                .select_edges(VIEWER, &[query(direction, destination)])
                .await;
            assert_eq!(
                answers
                    .first()
                    .and_then(|batch| batch.hydrated(&destination)),
                Some(&Hydrated::Found(holds)),
                "{direction:?}/{destination}"
            );
        }
        assert_eq!(recording.misses(&sources), Vec::<String>::new());
    }
}
