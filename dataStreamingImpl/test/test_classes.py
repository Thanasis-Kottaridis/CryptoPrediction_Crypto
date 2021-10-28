# import sys
import utils.mongoConnector as mongoConnector

class Mammal(object) :

    def __init__(self, name, age, gender) :
        self.name = name
        self.age = age
        self.gender = gender

        self.__privateInfoHash = "{},{},{}".format(name, age, gender)

    def display_private_inf(self) :
        print(self.__privateInfoHash)

    def display_age(self) :
        print("Im {} years old".format(self.age))

    def display_gender(self) :
        print("Im a {}".format(self.gender))

    @staticmethod
    def survive() :
        print("My Target is to survive")


class Dog(Mammal) :
    attr1 = "mammal"

    def __init__(self, name, age, gender, kind) :
        # instantiate parent class properties
        super().__init__(name, age, gender)
        self.kind = kind

    def bark(self) :
        print("wOOF")

    def survive(self) :
        print("my Target is to survive and serve people")

    def display_dog_private_inf(self):
        # print(self.__privateInfoHash) this doesnt exist
        print("cant access __privateInfoHash property of mammal")


obj = Dog("azor", 12, "male", "cho cho")
print(obj.attr1)
obj.display_age()
obj.display_gender()
obj.bark()
obj.survive()
obj.display_dog_private_inf()
obj.display_private_inf()

obj.favorite_food = "bones"
print(obj.favorite_food)

Mammal.survive()  # this method is execuring without object because it is static
# Mammal.display_gender() this can't be execute because method is not static

# print(sys.path)
mongoConnector.connectMongoDB()