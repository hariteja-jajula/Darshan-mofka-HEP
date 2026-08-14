pybind11_add_module (pydiaspora_stream_api py-diaspora-stream-api.cpp)
target_link_libraries (pydiaspora_stream_api PRIVATE diaspora-stream-api coverage_config)

set (PY_VERSION ${Python3_VERSION_MAJOR}.${Python3_VERSION_MINOR})

if (NOT DEFINED SKBUILD_PROJECT_NAME)
    install (TARGETS pydiaspora_stream_api
             EXPORT diaspora-stream-api-targets
             ARCHIVE DESTINATION lib/python${PY_VERSION}/site-packages
             LIBRARY DESTINATION lib/python${PY_VERSION}/site-packages)
    install (DIRECTORY diaspora_stream
             DESTINATION lib/python${PY_VERSION}/site-packages)
else ()
    install (TARGETS pydiaspora_stream_api DESTINATION .)
endif ()
