add_library (diaspora-simple-backend SHARED SimpleBackend.cpp)
target_link_libraries (diaspora-simple-backend PUBLIC diaspora-stream-api)

set (python-module-dir ${CMAKE_SOURCE_DIR}/python)
file (GLOB_RECURSE python-test-files "${python-module-dir}/test_*.py")

foreach (python-test-filename ${python-test-files})
    file (RELATIVE_PATH python-test-filename-rel ${python-module-dir} ${python-test-filename})
    string (REPLACE ".py" "" python-test-filename-base ${python-test-filename-rel})
    string (REPLACE "/" "." python-test-name ${python-test-filename-base})
    if (${ENABLE_COVERAGE})
        message (STATUS "${python-test-name} test will run with code coverage")
        add_test (NAME ${python-test-name} COMMAND coverage run -a -m unittest ${python-test-name})
    else ()
        add_test (NAME ${python-test-name} COMMAND python -m unittest ${python-test-name})
    endif ()
    set (python-path-env ${CMAKE_SOURCE_DIR}/python/:${CMAKE_BINARY_DIR}/python:$ENV{PYTHONPATH})
    set (ld-library-path-env ${CMAKE_BINARY_DIR}/src:${CMAKE_BINARY_DIR}/tests/python:$ENV{LD_LIBRARY_PATH})
    set_property (TEST ${python-test-name} PROPERTY ENVIRONMENT
                  PYTHONPATH=${python-path-env};LD_LIBRARY_PATH=${ld-library-path-env})
endforeach ()

install (TARGETS diaspora-simple-backend
         ARCHIVE DESTINATION lib
         LIBRARY DESTINATION lib)
