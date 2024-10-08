FROM python:3.11-alpine as build

WORKDIR /home/devman

RUN apk add --no-cache gcc libc-dev g++
RUN pip install --no-cache-dir pipenv
COPY Pipfile* ./
RUN pipenv install --system --clear

FROM python:3.11-alpine
COPY --from=build /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
WORKDIR /home/devman

COPY . .

USER 1337:1337
ENTRYPOINT [ "python3", "flowman.py"]
