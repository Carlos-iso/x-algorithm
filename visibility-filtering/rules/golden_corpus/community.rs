use super::{Role, Row};
use crate::models::{CommunityModeration, HydratedTweetCandidate, TweetFeatures, Verdict};
use crate::rules::fixtures::{allow, candidate, dropped};
use crate::rules::SafetyLevel::TimelineHomeHydration;
use std::num::NonZeroU64;
use xai_visibility_filtering::models::FilteredReason;

const HIDDEN: CommunityModeration = CommunityModeration {
    is_hidden: true,
    is_author_removed: false,
};

const AUTHOR_REMOVED: CommunityModeration = CommunityModeration {
    is_hidden: false,
    is_author_removed: true,
};

fn community_post(
    moderation: CommunityModeration,
    viewer_is_moderator: Option<bool>,
) -> HydratedTweetCandidate {
    let mut post = candidate()
        .with_tweet_features(TweetFeatures {
            community_id: NonZeroU64::new(500),
            ..Default::default()
        })
        .build();
    post.community_moderation = moderation;
    post.viewer_is_community_moderator = viewer_is_moderator;
    post
}

fn hidden_drop() -> Verdict {
    dropped(
        FilteredReason::UnspecifiedReason,
        "hidden_community_tweet/drop/unspecified",
    )
}

fn author_removed_drop() -> Verdict {
    dropped(
        FilteredReason::UnspecifiedReason,
        "author_removed_community_tweet/drop/unspecified",
    )
}

pub(super) fn rows() -> Vec<Row> {
    vec![
        Row {
            name: "hidden_community_post",
            post: community_post(HIDDEN, Some(false)),
            expect: vec![
                (TimelineHomeHydration, Role::NonFollower, hidden_drop()),
                (TimelineHomeHydration, Role::Follower, hidden_drop()),
                (TimelineHomeHydration, Role::LoggedOut, hidden_drop()),
            ],
        },
        Row {
            name: "hidden_community_post_moderator_lookup_failed",
            post: community_post(HIDDEN, None),
            expect: vec![
                (TimelineHomeHydration, Role::NonFollower, allow()),
                (TimelineHomeHydration, Role::LoggedOut, hidden_drop()),
            ],
        },
        Row {
            name: "author_removed_community_post",
            post: community_post(AUTHOR_REMOVED, Some(false)),
            expect: vec![
                (
                    TimelineHomeHydration,
                    Role::NonFollower,
                    author_removed_drop(),
                ),
                (TimelineHomeHydration, Role::Follower, author_removed_drop()),
                (
                    TimelineHomeHydration,
                    Role::LoggedOut,
                    author_removed_drop(),
                ),
            ],
        },
        Row {
            name: "hidden_author_removed_community_post",
            post: community_post(
                CommunityModeration {
                    is_hidden: true,
                    is_author_removed: true,
                },
                Some(false),
            ),
            expect: vec![(TimelineHomeHydration, Role::NonFollower, hidden_drop())],
        },
        Row {
            name: "unmoderated_community_post",
            post: community_post(CommunityModeration::default(), None),
            expect: vec![
                (TimelineHomeHydration, Role::NonFollower, allow()),
                (TimelineHomeHydration, Role::LoggedOut, allow()),
            ],
        },
    ]
}
