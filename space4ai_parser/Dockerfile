FROM python:3.8-slim

# create working directory
WORKDIR /space4ai-parser

# install python requirements
COPY requirements.txt .
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

# copy parser code to the working directory
COPY . .

# entrypoint
CMD ["python", "test_parser.py", "--help"]
