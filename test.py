import os

data = os.getenv("TEST")

print(data)



if isinstance(data, str):
    print("1")
    
else:
    print("2")
    
    
