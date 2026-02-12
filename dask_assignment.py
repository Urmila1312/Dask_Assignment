import pandas as pd
import dask.dataframe as dd
import random
from faker import Faker

fake = Faker()

# Step 1: Generate 1 Million Rows
num_rows = 1000000

print("Generating CSV file...")

data = {
    "id": range(1, num_rows + 1),
    "name": [fake.name() for _ in range(num_rows)],
    "age": [random.randint(21, 60) for _ in range(num_rows)],
    "salary": [random.randint(20000, 150000) for _ in range(num_rows)],
    "department": [random.choice(["HR", "IT", "Finance", "Marketing", "Sales"]) for _ in range(num_rows)]
}

df = pd.DataFrame(data)
df.to_csv("employees.csv", index=False)

print("CSV file created successfully!")

# Step 2: Load using Dask
dask_df = dd.read_csv("employees.csv")

# Step 3: Display first 10 rows
print("\nFirst 10 rows:")
print(dask_df.head(10))

# Step 4: Show number of partitions
print("\nNumber of partitions:")
print(dask_df.npartitions)
