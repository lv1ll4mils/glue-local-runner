FROM amazon/aws-glue-libs:glue_libs_4.0.0_image_01

RUN python3 -m pip install \
    google-api-python-client \
    google-auth-oauthlib \
    google-auth-httplib2 \
    fastparquet \
    workalendar \
    holidays

# Directorio de trabajo (donde se montará el proyecto local)
WORKDIR /workspace
