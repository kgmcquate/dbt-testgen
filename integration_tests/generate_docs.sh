dbt docs generate

mkdir -p ../docs
cp target/catalog.json ../docs
cp target/index.html ../docs
cp target/manifest.json ../docs
cp target/run_results.json ../docs