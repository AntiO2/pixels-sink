check_fatal_exit() {
    [[ $? -ne 0 ]] && { log_fatal_exit "$@";}
}

check_return() {
    [[ $? -ne 0 ]] && { log_warning "$@" && exit 1; }
}



gen_dir_md5sum() {
    local md5dir_path=$1
    if [ -z ${md5dir_path} ]; then
        md5dir_path="."
    fi
    [[ -d ${md5dir_path} ]] || { check_return "${md5dir_path} is not dir"; }
    find ${md5dir_path} -maxdepth 1  -type f| xargs md5sum  > ${md5dir_path}/md5sum.txt
}