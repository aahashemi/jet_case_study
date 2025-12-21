
echo "Stopping containers..."
docker-compose down

echo "Removing old image..."
docker rmi jet_case_study:latest

echo "Building new image..."
docker build -t jet_case_study:latest . --no-cache --progress=plain

echo "Starting containers..."
docker-compose up