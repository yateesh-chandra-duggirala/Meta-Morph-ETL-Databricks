from fastapi.middleware.cors import CORSMiddleware
import uvicorn
import pandas as pd
from os import environ as env
import random
from faker import Faker
from datetime import datetime
from google.cloud import storage
import io
from fastapi import FastAPI

# today = datetime.now().strftime("%Y%m%d")
today = "20250323"
print(today)
key_path = "meta-morph-d-eng-pro-admin.json"

app = FastAPI()
fake = Faker()

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



async def generate_data():

    NUM_SUPPLIERS_SAMPLE = random.randint(500,700)
    NUM_PRODUCTS_SAMPLE = random.randint(5000,8000)
    NUM_CUSTOMERS_SAMPLE = random.randint(15000,30000)
    NUM_SALES_SAMPLE = random.randint(200000,400000)

    # Generate Suppliers Data (Sample)
    print("Supplier Data Generation in progress....")
    suppliers_ids = [f"S{str(i).zfill(4)}" for i in range(1, NUM_SUPPLIERS_SAMPLE + 1)]
    random.shuffle(suppliers_ids)
    suppliers_sample = [
        {
            "supplier_id": suppliers_id,
            "supplier_name": fake.company(),
            "contact_details": fake.phone_number(),
            "region": random.choice(["North", "South", "East", "West", "Central"]),
        }
        for suppliers_id in suppliers_ids
    ]
    print(f"*-*-*-*- Supplier Dataset Generated with : {NUM_SUPPLIERS_SAMPLE} records -*-*-*-*-*")
    await gs_bucket_auth_save(suppliers_sample, "supplier")

    # Generate Products Data (Sample)
    print("Products Data Generation in progress....")
    products_ids = [f"P{str(i).zfill(5)}" for i in range(1, NUM_PRODUCTS_SAMPLE + 1)]
    random.shuffle(products_ids) 

    products_sample = [
        {
            "product_id": product_id, 
            "product_name": f"Product {chr(65 + (i % 26))}{i}",
            "category": random.choice(["Electronics", "Apparel", "Home", "Books", "Toys"]),
            "price": round(random.uniform(5, 1000), 2),
            "stock_quantity": random.randint(50, 1000),
            "reorder_level": random.randint(10, 50),
            "supplier_id": f"S{str(random.randint(1, NUM_SUPPLIERS_SAMPLE)).zfill(4)}",
        }
        for i, product_id in enumerate(products_ids, start=1)
    ]

    print(f"*-*-*-*- Products Dataset Generated with : {NUM_PRODUCTS_SAMPLE} records -*-*-*-*-*")
    await gs_bucket_auth_save(products_sample, "product")

    # Generate Customers Data (Sample)
    print("Customers Data Generation in progress....")
    customer_ids = [f"C{str(i).zfill(5)}" for i in range(1, NUM_CUSTOMERS_SAMPLE + 1)]
    random.shuffle(customer_ids)

    customers_sample = [
        {
            "customer_id": customer_id,
            "name": (name := fake.name()),
            "city": fake.city(),
            "email": name.lower().replace(" ", "") + "@" + random.choice(["yahoo.com", "gmail.com", "outlook.com"]),
            "phone_number": fake.phone_number(),
            "loyalty_tier": random.choice(["Bronze", "Silver", "Gold", "Platinum"]),
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
            "sale_id": sale_id,
            "customer_id": f"C{str(random.randint(1, NUM_CUSTOMERS_SAMPLE)).zfill(5)}",
            "product_id": product["product_id"],
            "sale_date": random.choice([fake.date_between(start_date="-2y", end_date="today").strftime("%Y-%m-%d"), ""]),
            "quantity": quantity,
            "discount": discount,
            "shipping_cost": shipping_cost,
            "order_status": order_status,
            "payment_mode": payment_mode,
        })
    print(f"*-*-*-*- Sales Dataset Generated with : {NUM_SALES_SAMPLE} records -*-*-*-*-*")
    await gs_bucket_auth_save(sales_sample, "sales")


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


# suppliers API to fetch the latest supplier data from the meta-morph bucket
@app.get("/v1/suppliers")
async def load_suppliers_data():

    df = pd.read_csv(f"gs://meta-morph/{today}/supplier_{today}.csv", 
                        storage_options={
                            "token": key_path
                        }
                    ).set_index("supplier_id")

    supplier_result = df.reset_index().to_dict(orient="records")
    return {"status" : 200, "data" : supplier_result}

# products API to fetch the latest product data from the meta-morph bucket
@app.get("/v1/products")
async def load_products_data():

    df = pd.read_csv(f"gs://meta-morph/{today}/product_{today}.csv", 
                        storage_options={
                            "token": key_path
                        }
                    ).set_index("product_id")

    product_result = df.reset_index().to_dict(orient="records")
    return {"status" : 200, "data" : product_result}

# customers API to fetch the latest customer data from the meta-morph bucket
@app.get("/v1/customers")
async def load_customer_data():

    df = pd.read_csv(f"gs://meta-morph/{today}/customer_{today}.csv", 
                        storage_options={
                            "token": key_path
                        }
                    ).set_index("customer_id")

    customer_result = df.reset_index().to_dict(orient="records")
    return {"status" : 200, "data" : customer_result}

# Setting up the Cors Origin
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)