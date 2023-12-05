FROM ubuntu:20.04 AS base
ENV DEBIAN_FRONTEND noninteractive
RUN apt-get update && apt-get install -y graphviz \
			graphviz-dev \
			doxygen nano \
			git \
        		python3-dev \
        		python3-pip
RUN python3 -m pip install --upgrade pip

WORKDIR /home/SPACE4AI-D

COPY ./Run_and_Evaluate_integrate_AISPRINT.py .
COPY ./classes ./classes
COPY ./Solid ./Solid
COPY ./requirements.txt .

# Install requirements for the SPACE4AI-D program
RUN pip install -r ./requirements.txt

# define parser, logger and aMLlibrary url
ENV GITLAB=https://gitlab.polimi.it
ENV PARSER_URL=${GITLAB}/ai-sprint/space4ai-parser.git
ENV LOGGER_URL=${GITLAB}/ai-sprint/space4ai-logger.git
ENV aMLLibrary_URL=${GITLAB}/ai-sprint/a-mllibrary.git
ENV PROJECT_ID=776
ENV PARSER_DIR=external/space4ai_parser
ENV LOGGER_DIR=external/space4ai_logger
ENV aMLLibrary_DIR=aMLLibrary

# install aMLLibrary
RUN git clone --recurse-submodules ${aMLLibrary_URL} ./${aMLLibrary_DIR}
RUN pip install -r ./${aMLLibrary_DIR}/requirements.txt

# install logger
RUN git clone ${LOGGER_URL} ./${LOGGER_DIR}

############################################################################
#			build image for development
############################################################################
FROM base as image-dev

# copy the last change from your brach to invalidate the cache if there
# was a new change
ADD "${GITLAB}/api/v4/projects/${PROJECT_ID}/repository/branches/main" \
	/tmp/devalidateCache

# install parser (latest version)
RUN git clone ${PARSER_URL} ./${PARSER_DIR}
RUN pip install --no-cache-dir -r ./${PARSER_DIR}/requirements.txt

# entrypoint
CMD bash

############################################################################
#			build image for production
############################################################################
FROM base as image-prod

# define parser tag
ARG PARSER_TAG=23.06.30

# install parser
RUN git clone	--depth 1 \
		--branch ${PARSER_TAG} \
		${PARSER_URL} \
		./${PARSER_DIR}
RUN pip install --no-cache-dir -r ./${PARSER_DIR}/requirements.txt

# entrypoint
CMD bash

