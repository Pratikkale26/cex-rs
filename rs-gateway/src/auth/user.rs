/// A user stored in the gateway's in-memory list.
#[derive(Debug, Clone)]
pub struct User {
    pub id:       u64,
    pub username: String,
    pub password: String,
}
