use iceberg::{Catalog, TableCreation, TableIdent};
use iceberg_catalog_rest::{RestCatalog, RestCatalogConfig};

#[tokio::main]
async fn main() {
    println!("Hello, world!");
    // Create catalog
    let config = RestCatalogConfig::builder()
    .uri("http://localhost:8080".to_string())
    .build();

    let catalog = RestCatalog::new(config);
    let all_namespaces = catalog.list_namespaces(None).await.unwrap();
    println!("Namespaces in current catalog: {:?}", all_namespaces);

    let table2 = catalog
        .load_table(&TableIdent::from_strs(["nyc", "taxis"]).unwrap())
        .await
        .unwrap();
    println!("{:?}", table2.metadata());

}
