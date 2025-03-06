# Rust Database Engine for FYP

## Overview


An abstract syntax tree (AST) is a data structure used in computer science to represent the structure of a program or code snippet.

## Create Table

## Gets

## Inserts

## Updates

## Deletion

Rows sent to delete call such as 

``
DELETE FROM test_table WHERE uuid = \"ec32b2da-6344-4d85-83bf-a831d69f3f40\"
``

Will set all Int values to 0 , timestamps to time of deletion, all String values to ROW_REMOVED.


### Docker Commands
Build Command:
``
docker build -t final-year-project-database-rust . 
``

Run Container on Local Host (Change Port Bindings if config is changed):
``
docker run -p 8080:8080 final-year-project-database-rust
``

Docker Run with custom container name:
``
docker run --name my-rust-database-8081 -p 8081:8081 -e PORT=8081 final-year-project-database-rust
``

Tagging for docker hub:
``
docker tag final-year-project-database-rust adam0mah/rust-db-engine:test-ver-1
``

Pushing to docker Hub:
``
docker push adam0mah/rust-db-engine:test-ver-1
``
