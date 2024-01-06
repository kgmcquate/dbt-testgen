dbt docs generate

sed -i  s"|<title>dbt Docs</title>|<title>dbt-testgen Docs</title><link rel='stylesheet' href='/dbt-testgen/styles.css' />|"g target/index.html

mkdir -p ../docs
cp target/catalog.json ../docs
cp target/index.html ../docs
cp target/manifest.json ../docs
cp target/run_results.json ../docs
cp styles.css ../docs