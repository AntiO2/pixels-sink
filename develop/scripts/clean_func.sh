

shutdown_containers() {
    docker-compose  -f ${PROJECT_DIR}/docker-compose.yml down -v
}