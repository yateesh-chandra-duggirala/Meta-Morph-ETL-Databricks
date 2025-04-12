from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
from os import environ as env
import random
from faker import Faker
from datetime import datetime
from google.cloud import storage
import io
from fastapi import FastAPI, Depends
from auth_utils import *

# today = datetime.now().strftime("%Y%m%d")
today = "20250330"
print(today)
key_path = "meta-morph-d-eng-pro-admin.json"

# Create an object app for the FastAPI class
app = FastAPI()

# Create an object app for the Faker class to get Indian encoded Data
fake = Faker("en_IN")

# This pulls the data from the Postgres Database
def get_data(relation, cnt):
    import psycopg2

    # Establish a connection with the postgres
    conn = psycopg2.connect(
        database = 'meta_morph',
        user = 'postgres',
        password = 'postgres',
        host = 'localhost',
        port = '5432'
    )

    # Create a cursor Object
    cursor = conn.cursor()

    # Define a SQL Query with Insertion statements.
    get_sql = f'''
                select * from server.{relation}
                LIMIT {cnt}
                '''

    # Execute the Query with the help of the cursor object.
    cursor.execute(get_sql)
    result = cursor.fetchall()
    conn.close()
    return result

# Saves the data in the GCS Bucket
async def gs_bucket_auth_save(sample, type_of_data):
    csv_buffer = io.BytesIO()
    print(f"Writing the {type_of_data} file into Bucket")
    pd.DataFrame(sample).to_csv(csv_buffer, index=False)

    # Upload the content of BytesIO object to GCS
    storage_client = storage.Client.from_service_account_json(key_path)
    bucket_name = "meta-morph"
    bucket = storage_client.bucket(bucket_name)
    destination_blob_name = f"{today}/{type_of_data}_{today}.csv"
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_string(csv_buffer.getvalue(), content_type="text/csv", timeout=300)

    print(f"*-*-*-*- File uploaded to {bucket_name}/{destination_blob_name} -*-*-*-*-*\n\n")


# This is the function to generate the data
async def generate_data():

    NUM_SUPPLIERS_SAMPLE = 252
    NUM_PRODUCTS_SAMPLE = random.randint(400,480)
    NUM_CUSTOMERS_SAMPLE = random.randint(15000,30000)
    NUM_SALES_SAMPLE = random.randint(200000,400000)

    # Generate Suppliers Data (Sample)
    print("Supplier Data Generation in progress....")
    suppliers_from_db = get_data("supplier",NUM_SUPPLIERS_SAMPLE)

    suppliers_sample = [
        {
            "Supplier Id": row[0],
            "Supplier Name": row[1],
            "Contact Details": row[2],
            "Region": row[3],
        }
        for row in suppliers_from_db[:NUM_SUPPLIERS_SAMPLE]
    ]
    print(f"*-*-*-*- Supplier Dataset Generated with : {NUM_SUPPLIERS_SAMPLE} records -*-*-*-*-*")
    await gs_bucket_auth_save(suppliers_sample, "supplier")

    # Generate Products Data (Sample)
    print("Products Data Generation in progress....")
    products_from_db = get_data("product",NUM_PRODUCTS_SAMPLE)

    products_sample = [
        {
            "Product Id": row[0],
            "Product Name": row[1],
            "Category": row[2],
            "Price": round(random.uniform(5, 1000), 2),
            "Stock Quantity": random.randint(50, 1000),
            "Reorder Level": random.randint(10, 50),
            "Supplier Id": f"S{str(random.randint(1, NUM_SUPPLIERS_SAMPLE)).zfill(4)}",
        }
        for row in products_from_db[:NUM_PRODUCTS_SAMPLE]
    ]

    print(f"*-*-*-*- Products Dataset Generated with : {NUM_PRODUCTS_SAMPLE} records -*-*-*-*-*")
    await gs_bucket_auth_save(products_sample, "product")

    # Generate Customers Data (Sample)
    print("Customers Data Generation in progress....")
    customer_ids = [f"C{str(i).zfill(5)}" for i in range(1, NUM_CUSTOMERS_SAMPLE + 1)]
    random.shuffle(customer_ids)

    customers_sample = [
        {
            "Customer Id": customer_id,
            "Name": (name := fake.name()),
            "City": fake.city(),
            "Email": name.lower().replace(" ", "") + "@" + random.choice(["yahoo.com", "gmail.com", "outlook.com"]),
            "Phone Number": str(random.choice([6, 7, 8, 9])) + ''.join(str(random.randint(0, 9)) for _ in range(9)),
        }
        for customer_id in customer_ids
    ]
    print(f"*-*-*-*- Customers Dataset Generated with : {NUM_CUSTOMERS_SAMPLE} records -*-*-*-*-*")
    await gs_bucket_auth_save(customers_sample, "customer")

    # Generate Sales Data (Sample)
    print("Sales Data Generation in progress....")
    sales_sample = []
    sale_ids = list(range(1, NUM_SALES_SAMPLE + 1))
    random.shuffle(sale_ids)

    for sale_id in sale_ids:
        product = random.choice(products_sample)
        quantity = random.randint(1, 20)
        discount = round(random.uniform(0, 50), 2)
        # sale_price = product["price"] * quantity * (1 - discount / 100)
        shipping_cost = round(random.uniform(5, 50), 2)
        order_status = random.choice(["Pending", "Shipped", "Delivered", "Cancelled"])
        payment_mode = random.choice(["Credit Card", "Debit Card", "UPI", "Cash on Delivery"])

        sales_sample.append({
            "Sale Id": sale_id,
            "Customer Id": f"C{str(random.randint(1, NUM_CUSTOMERS_SAMPLE)).zfill(5)}",
            "Product Id": product["Product Id"],
            "Sale Date": random.choice([fake.date_between(start_date="-2y", end_date="today").strftime("%Y-%m-%d"), ""]),
            "Quantity": quantity,
            "Discount": discount,
            "Shipping Cost": shipping_cost,
            "Order Status": order_status,
            "Payment Mode": payment_mode,
        })
    print(f"*-*-*-*- Sales Dataset Generated with : {NUM_SALES_SAMPLE} records -*-*-*-*-*")
    await gs_bucket_auth_save(sales_sample, "sales")


# Checks if the files are available for that day
def files_check():

    # Authenticate using the service account
    client = storage.Client.from_service_account_json(key_path)

    # List files in the bucket
    bucket = client.get_bucket("meta-morph")
    blobs = bucket.list_blobs()
    server_ready = False

    for blob in blobs:
        if blob.name.split("/")[0] == today:
            server_ready = True
            break
    if not server_ready:
        raise BaseException("Failed to fetch files")

    return server_ready

@app.on_event("startup")
async def startup_event():
    try:
        files_check()
        print("Fetched files successfully")
    except BaseException:
        print("Please Hold on, Files are not found.! Fetching from SAP")
        await generate_data()        


@app.get("/")
async def do_wish():
    return {"status" : 200, "message" : f"You are accessing {env['MY_VARIABLE']} environment"}

# Generate a token by calling this API
@app.get("/v1/token")
def generate_token():
    data = {"sub": "email"} 
    token = create_access_token(data)
    return {"access_token": token}

# suppliers API to fetch the latest supplier data from the meta-morph bucket
@app.get("/v1/suppliers")
async def load_suppliers_data():

    df = pd.read_csv(f"gs://meta-morph/{today}/supplier_{today}.csv", 
                        storage_options={
                            "token": key_path
                        }
                    ).set_index("Supplier Id")

    supplier_result = df.reset_index().to_dict(orient="records")
    return {"status" : 200, "data" : supplier_result}

# products API to fetch the latest product data from the meta-morph bucket
@app.get("/v1/products")
async def load_products_data():

    df = pd.read_csv(f"gs://meta-morph/{today}/product_{today}.csv", 
                        storage_options={
                            "token": key_path
                        }
                    ).set_index("Product Id")

    product_result = df.reset_index().to_dict(orient="records")
    return {"status" : 200, "data" : product_result}

# customers API to fetch the latest customer data from the meta-morph bucket
@app.get("/v1/customers")
async def load_customer_data(payload: dict = Depends(verify_token)):
    df = pd.read_csv(
        f"gs://meta-morph/{today}/customer_{today}.csv",
        storage_options={"token": key_path}
    ).set_index("Customer Id")

    customer_result = df.reset_index().to_dict(orient="records")
    return {"status": 200, "data": customer_result}

# Setting up the Cors Origin
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)