#!/bin/bash

usage() {
  cat <<EOF


EOF
  exit 0;
}


develop_debug=off
need_init=on
for arg do
  val=`echo "$arg" | sed -e 's;^--[^=]*=;;'`

  case "$arg" in
    --develop_debug=*)          develop_debug="$val";;
    --need_init=*)              need_init="$val";;
    -h|--help)                  usage ;;
    *)                          echo "wrong options : $arg";
                                exit 1
                                ;;
  esac
done

[[ x${develop_debug} == x"on" ]] && { set -x;}

BASH_PATH=`readlink -f $0`
SCRIPT_DIR=`dirname ${BASH_PATH}`



source ${SCRIPT_DIR}/common_func.sh


if [[ x${need_init} == x"on" ]]; then
  log_info "Init Container"
  docker-compose -f ${PROJECT_DIR}/docker-compose.yml up -d
  check_fatal_return "docker-compose up failed."
fi


log_info "Create Schema"



