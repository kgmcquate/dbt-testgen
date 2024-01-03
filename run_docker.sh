docker build . -t dbt-testgen
docker run -it -p 8080:8080 -v ${PWD}:/run/dbt-testgen dbt-testgen