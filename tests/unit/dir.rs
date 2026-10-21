use std::cmp::Ordering;

use uuid::Uuid;

use super::Name;

#[test]
fn name_partial_cmp_uuid() {
    let uuid = Uuid::new_v4();
    let key = uuid.to_string();

    assert_eq!(
        Name::partial_cmp(&uuid, key.as_str()),
        Some(Ordering::Equal)
    );
    assert_eq!(Name::partial_cmp(&uuid, "not-a-uuid"), None);
}
