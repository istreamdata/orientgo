# asdf





# Cheat Sheet of Common Use Cases

## Creation and Connection

### How to create a test/dummy database

    create database plocal:../databases/cars admin admin plocal

### Connect to database

    connect remote:localhost/cars admin admin
    
### Create a class with properties and put a record in

    create class Person
    create property Person.name string
    insert into Person (name) values('Luke')
    info class Person
    select from Person
    load record #11:0  (or whatever the rid is)
    

----

## Indexes

### See all indexes

    select expand(indexes) from metadata:indexmanager

----

### Users and Permissions

#### Create new user with write privileges

    insert into ouser set name = 'midpeter444', password = 'jiffylube', status = 'ACTIVE', roles = (select from
    ORole where name = 'writer)
    
   
#### Change password of user

    update ouser set password = 'hello' where name = 'midpeter444'
