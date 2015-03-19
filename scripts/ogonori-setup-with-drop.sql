#Create DATABASE remote:localhost/assets root xxxx  plocal
#CREATE CLASS Application extends V
#CREATE CLASS Asset extends Application
#create property Asset.addresses embeddedlist

drop database remote:localhost/ogonoriTest root jiffylube plocal
CREATE DATABASE remote:localhost/ogonoriTest root jiffylube plocal
CREATE CLASS Animal
CREATE property Animal.name string
CREATE property Animal.age integer
CREATE CLASS Cat extends Animal
CREATE property Cat.caretaker string

INSERT INTO Cat (name, age, caretaker) VALUES ("Linus", 15, "Michael"), ("Keiko", 10, "Anna")
