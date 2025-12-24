
docker-compose down

docker rmi jet_case_study:latest

docker build -t jet_case_study:latest . --no-cache --progress=plain

docker-compose up