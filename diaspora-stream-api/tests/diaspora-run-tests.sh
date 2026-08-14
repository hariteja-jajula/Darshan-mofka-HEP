#!/usr/bin/env bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

BEFORE_COMMAND=""
AFTER_COMMAND=""
RET=0
MODE="all"
MODE_ARG=""

while [[ $# -gt 0 ]]; do
    key="$1"
    case $key in
        --backend-args)
        export DIASPORA_TEST_BACKEND_ARGS="$2"
        shift # past argument
        shift # past value
        ;;
        --topic-args)
        export DIASPORA_TEST_TOPIC_ARGS="$2"
        shift # past argument
        shift # past value
        ;;
        --before)
        BEFORE_COMMAND="$2"
        shift # past argument
        shift # past value
        ;;
        --after)
        AFTER_COMMAND="$2"
        shift # past argument
        shift # past value
        ;;
        --list-binary-tests)
        MODE="list-binary"
        shift
        ;;
        --list-python-tests)
        MODE="list-python"
        shift
        ;;
        --run-binary)
        MODE="run-binary"
        MODE_ARG="$2"
        shift # past argument
        shift # past value
        ;;
        --run-python)
        MODE="run-python"
        MODE_ARG="$2"
        shift # past argument
        shift # past value
        ;;
        --*)
        echo "Unknown option: $1" >&2
        exit 1
        ;;
        *)    # positional: backend name
        export DIASPORA_TEST_BACKEND="$1"
        shift # past argument
        ;;
    esac
done

run_binary_test() {

    echo "DIASPORA_TEST_BACKEND: ${DIASPORA_TEST_BACKEND}"
    echo "DIASPORA_TEST_BACKEND_ARGS: ${DIASPORA_TEST_BACKEND_ARGS}"
    echo "DIASPORA_TEST_TOPIC_ARGS: ${DIASPORA_TEST_TOPIC_ARGS}"
    echo "BEFORE_COMMAND: ${BEFORE_COMMAND}"
    echo "AFTER_COMMAND: ${AFTER_COMMAND}"

    local test_file="$1"
    #if [[ ! $test_file = *DataSelection* ]]; then
    #    return
    #fi
    echo "Running test file ${test_file}"
    if [ -n "$BEFORE_COMMAND" ]; then
        eval "$BEFORE_COMMAND"
        if [ "$?" -ne 0 ]; then
            RET=1
        fi
    fi
    timeout 120s ${test_file}
    r=$?
    if [ "$r" -ne 0 ]; then
        RET=1
    fi
    if [ -n "$AFTER_COMMAND" ]; then
        eval "$AFTER_COMMAND $r"
        if [ "$?" -ne 0 ]; then
            RET=1
        fi
    fi
}

run_python_test() {

    echo "DIASPORA_TEST_BACKEND: ${DIASPORA_TEST_BACKEND}"
    echo "DIASPORA_TEST_BACKEND_ARGS: ${DIASPORA_TEST_BACKEND_ARGS}"
    echo "DIASPORA_TEST_TOPIC_ARGS: ${DIASPORA_TEST_TOPIC_ARGS}"
    echo "BEFORE_COMMAND: ${BEFORE_COMMAND}"
    echo "AFTER_COMMAND: ${AFTER_COMMAND}"

    local test_file="$1"
    echo "------------------------------------------------------------"
    echo "Running test: $test_file"
    echo "------------------------------------------------------------"
    local filename=$(basename $test_file)
    local test_name="diaspora_stream.${filename%.*}"
    if [ -n "$BEFORE_COMMAND" ]; then
        eval "$BEFORE_COMMAND"
        if [ "$?" -ne 0 ]; then
            RET=1
        fi
    fi
    LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$SCRIPT_DIR/../lib \
    timeout 120s python -m unittest -v $test_name
    r=$?
    if [ "$r" -ne 0 ]; then
        RET=1
    fi
    if [ -n "$AFTER_COMMAND" ]; then
        eval "$AFTER_COMMAND $r"
        if [ "$?" -ne 0 ]; then
            RET=1
        fi
    fi
}

get_python_test_dir() {
    LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$SCRIPT_DIR/../lib \
    PYTHONPATH=$PYTHONPATH:$SCRIPT_DIR/python \
    python -c "import diaspora_stream, os; print(os.path.dirname(os.path.abspath(diaspora_stream.__file__)))"
}

case $MODE in
    list-binary)
    for test_file in ${SCRIPT_DIR}/Diaspora*Test ; do
        basename "$test_file"
    done
    ;;

    list-python)
    DIASPORA_STREAM_PATH=$(get_python_test_dir)
    for test_file in $DIASPORA_STREAM_PATH/test_*.py; do
        basename "${test_file%.py}"
    done
    ;;

    run-binary)
    run_binary_test "${SCRIPT_DIR}/${MODE_ARG}"
    ;;

    run-python)
    DIASPORA_STREAM_PATH=$(get_python_test_dir)
    run_python_test "${DIASPORA_STREAM_PATH}/${MODE_ARG}.py"
    ;;

    all)
    # Binary tests
    for test_file in ${SCRIPT_DIR}/Diaspora*Test ; do
        run_binary_test "$test_file"
    done

    # Python tests
    DIASPORA_STREAM_PATH=$(get_python_test_dir)
    for test_file in $DIASPORA_STREAM_PATH/test_*.py; do
        run_python_test "$test_file"
    done
    ;;
esac

exit $RET
