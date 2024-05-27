class Solution:
    def __init__(self, name, age):
        self.name = name
        self.age = age

    def __repr__(self):
        return f"{self.name} is {self.age} years old"
    
def main():
    s = Solution("John", 30)
    s2 = Solution("Jane", 28)
    print(s, "and",  s2)

if __name__ == "__main__":
    main()