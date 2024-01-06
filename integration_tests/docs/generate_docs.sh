dbt docs generate

# Inject custom stylesheet
sed -i  \
    s"|<title>dbt Docs</title>|<title>dbt-testgen Docs</title><link rel='stylesheet' href='/dbt-testgen/styles.css' />|"g \
    target/index.html

mkdir -p ./static/
cp target/catalog.json ./static/
cp target/index.html ./static/
cp target/manifest.json ./static/
cp target/run_results.json ./static/
cp docs/styles.css ./static/