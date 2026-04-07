pub mod postgres;

use super::provider::DatabaseProvider;

pub fn get_provider(name: &str) -> Option<Box<dyn DatabaseProvider>> {
    match name {
        "postgres" | "postgresql" => Some(Box::new(postgres::PostgresProvider::new())),
        _ => None,
    }
}
