shutdown_containers() {
    docker-compose  -f ${PROJECT_DIR}/docker-compose.yml down -v
}

clean_images() {
    local images=("pixels-debezium:${PIXELS_SINK_VERSION}")

    for image in "${images[@]}"; do
        log_info "Deleting image: $image"
        docker rmi -f "$image" 2>/dev/null || echo "Failed to delete image: $image"
    done
}


build_image() {
    local BUILD_DIR=$1
    local IMAGE_NAME=$2
    local IMAGE_VERSION=${3:-${PIXELS_SINK_VERSION}}
    if [ -z "$BUILD_DIR" ]; then
        log_fatal_exit "Please provide a directory to build the image."
    fi
    if [ ! -d "$BUILD_DIR" ]; then
        log_fatal_exit "Directory '$BUILD_DIR' does not exist."
    fi

    if [ -z "$IMAGE_NAME" ]; then 
        log_fatal_exit "image name is empty"
    fi

    log_info "Building Docker image ${IMAGE_NAME}:${IMAGE_VERSION} from directory: $BUILD_DIR"
    docker build -t ${IMAGE_NAME}:${IMAGE_VERSION} $BUILD_DIR

    check_fatal_exit "Failed to build Docker image."
    
    log_info "Succ Build ${IMAGE_NAME}:${IMAGE_VERSION}"
}
