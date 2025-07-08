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
from my_secrets import *

today = datetime.now().strftime("%Y%m%d")
print(today)

# Create an object app for the FastAPI class
app = FastAPI()

# Create an object app for the Faker class to get Indian encoded Data
fake = Faker("en_IN")


# This pulls the data from the Postgres Database
def get_data(relation, cnt):
    """
    Fetches data from a specific relation (table) in the Postgres database.

    Parameters:
        relation (String): Table name to query from.
        cnt (int): Number of records to retrieve.

    Returns:
        list: Resulting records from the query.
    """
    import psycopg2

    # Establish a connection with the postgres
    conn = psycopg2.connect(
        database='meta_morph',
        user=USERNAME,
        password=PASSWORD,
        host='localhost',
        port='5432'
    )

    # Create a cursor Object
    cursor = conn.cursor()

    # Define a SQL Query with Insertion statements.
    get_sql = f'''
                select * from server.{relation}
                ORDER BY 1
                LIMIT {cnt}
                '''

    # Execute the Query with the help of the cursor object.
    cursor.execute(get_sql)
    result = cursor.fetchall()
    conn.close()
    return result


# Saves the data in the GCS Bucket
async def gs_bucket_auth_save(sample, type_of_data):
    """
    Saves a dataset as a CSV into a Google Cloud Storage bucket.

    Parameters:
        sample (list/dict): Data to be saved.
        type_of_data (String): Identifier for naming the file in GCS.
    """
    csv_buffer = io.BytesIO()
    print(f"Writing the {type_of_data} file into Bucket")
    pd.DataFrame(sample).to_csv(csv_buffer, index=False)

    # Upload the content of BytesIO object to GCS
    storage_client = storage.Client.from_service_account_json(SERVICE_KEY)
    bucket_name = "meta-morph-flow"
    bucket = storage_client.bucket(bucket_name)
    destination_blob_name = f"{today}/{type_of_data}_{today}.csv"
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_string(
                            csv_buffer.getvalue(),
                            content_type="text/csv",
                            timeout=300
                            )

    print(f"*-*-* File uploaded to {bucket_name} *-*-*\n\n")


# This is the function to generate the data
async def generate_data():
    """
    Generates synthetic data for suppliers, products, customers, and sales.
    Saves each dataset into Google Cloud Storage for the current day.
    """
    NUM_SUPPLIERS_SAMPLE = 252
    NUM_PRODUCTS_SAMPLE = random.randint(400, 483)
    NUM_CUSTOMERS_SAMPLE = random.randint(7005, 10032)
    NUM_SALES_SAMPLE = random.randint(50000, 120000)

    # Generate Suppliers Data (Sample)
    print("Supplier Data Generation in progress....")
    suppliers_from_db = get_data("supplier", NUM_SUPPLIERS_SAMPLE)

    suppliers_sample = [
        {
            "Supplier Id": row[0],
            "Supplier Name": row[1],
            "Contact Details": row[2],
            "Region": row[3],
        }
        for row in suppliers_from_db[:NUM_SUPPLIERS_SAMPLE]
    ]
    print(f"*-*-* Supplier Generated : {NUM_SUPPLIERS_SAMPLE} records *-*-*")
    await gs_bucket_auth_save(suppliers_sample, "supplier")

    # Generate Products Data (Sample)
    print("Products Data Generation in progress....")
    products_from_db = get_data("product", NUM_PRODUCTS_SAMPLE)

    supplier_id_list = random.sample(
        [supplier["Supplier Id"].strip() for supplier in suppliers_sample],
        k=random.randint(210, 225)
    )

    products_sample = [
        (
            lambda price=round(random.uniform(10, 700), 2): {
                "Product Id": row[0],
                "Product Name": row[1],
                "Category": row[2],
                "Selling Price": price,
                "Cost Price": round(price * random.uniform(0.45, 0.80), 2),
                "Stock Quantity": random.randint(6000, 12000),
                "Reorder Level": random.randint(10, 50),
                "Supplier Id": random.choice(supplier_id_list),
                                                             }
        )()
        for row in products_from_db[:NUM_PRODUCTS_SAMPLE]
    ]

    print(f"*-*-* Products Generated : {NUM_PRODUCTS_SAMPLE} records *-*-*")
    await gs_bucket_auth_save(products_sample, "product")

    # Generate Customers Data (Sample)
    print("Customers Data Generation in progress....")
    customer_from_db = get_data("customer", NUM_CUSTOMERS_SAMPLE)

    customers_sample = [
        {
            "Customer Id": row[0],
            "Name": row[1],
            "City": row[2],
            "Email": row[3],
            "Phone Number": row[4],
        }
        for row in customer_from_db[:NUM_CUSTOMERS_SAMPLE]
    ]
    print(f"*-*-* Customers Generated : {NUM_CUSTOMERS_SAMPLE} records *-*-*")
    await gs_bucket_auth_save(customers_sample, "customer")

    # Generate Sales Data (Sample)
    print("Sales Data Generation in progress....")
    sales_sample = []
    sale_ids = list(range(1, NUM_SALES_SAMPLE + 1))
    random.shuffle(sale_ids)

    product_id_list = random.sample(
        [product["Product Id"] for product in products_sample],
        k=random.randint(300, 370)
    )

    customer_id_list = random.sample(
        [customer["Customer Id"] for customer in customers_sample],
        k=random.randint(6800, 7000)
    )

    for sale_id in sale_ids:
        quantity = random.randint(1, 20)
        discount = round(random.uniform(0, 17), 2)
        shipping_cost = round(random.uniform(5, 50), 2)
        payment_mode = random.choice(
                ["Credit Card", "Debit Card", "UPI", "Cash on Delivery"]
                )

        # Generate realistic sale date
        sale_date_obj = fake.date_between(start_date="-2y", end_date="today")
        sale_date_str = sale_date_obj.strftime("%Y-%m-%d")
        days_ago = (datetime.today().date() - sale_date_obj).days

        # Conditional Order Status based on how recent the order is
        if days_ago <= 50:
            order_status = random.choices(
                                            ["Pending", "Shipped"],
                                            weights=[70, 30], k=1
                                         )[0]
        else:
            order_status = random.choices(
                                            ["Delivered", "Cancelled"],
                                            weights=[90, 10], k=1
                                          )[0]

        sales_sample.append({
            "Sale Id": sale_id,
            "Customer Id": random.choice(customer_id_list),
            "Product Id": random.choice(product_id_list),
            "Sale Date": sale_date_str,
            "Quantity": quantity,
            "Discount": discount,
            "Shipping Cost": shipping_cost,
            "Order Status": order_status,
            "Payment Mode": payment_mode,
        })
    print(f"*-*-* Sales Generated : {NUM_SALES_SAMPLE} records *-*-*")
    await gs_bucket_auth_save(sales_sample, "sales")


# Checks if the files are available for that day
def files_check():
    """
    Checks if today's files already exist in the GCS bucket.

    Returns:
        bool: True if files exist for today's date.

    Raises:
        BaseException: If no matching files are found in the bucket.
    """
    # Authenticate using the service account
    client = storage.Client.from_service_account_json(SERVICE_KEY)

    # List files in the bucket
    bucket = client.get_bucket("meta-morph-flow")
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
    """
    Triggered on app startup. Checks if today's files exist in GCS,
    otherwise generates data for suppliers, products, customers, and sales.
    """
    try:
        files_check()
        print("Fetched files successfully")
    except BaseException:
        print("Please Hold on, Files are not found.! Fetching from SAP")
        await generate_data()


@app.get("/")
async def do_wish():
    """
    Base endpoint to verify environment access.

    Returns:
        dict: Status and environment message.
    """
    return {"status": 200,
            "message": f"You are accessing {env['MY_VARIABLE']} environment"}


# Generate a token by calling this API
@app.get("/v1/token")
def generate_token():
    """
    API endpoint to generate an access token.

    Returns:
        dict: JWT access token.
    """
    data = {"sub": "email"}
    token = create_access_token(data)
    return {"access_token": token}


# suppliers API to fetch the latest supplier data from the bucket
@app.get("/v1/suppliers")
async def load_suppliers_data():
    """
    API endpoint to load latest supplier data from GCS bucket.

    Returns:
        dict: Supplier records.
    """
    df = pd.read_csv(f"gs://meta-morph-flow/{today}/supplier_{today}.csv",
                     storage_options={
                            "token": SERVICE_KEY
                        }
                     ).set_index("Supplier Id")

    supplier_result = df.reset_index().to_dict(orient="records")
    return {"status": 200, "data": supplier_result}


# products API to fetch the latest product data from the meta-morph-flow bucket
@app.get("/v1/products")
async def load_products_data():
    """
    API endpoint to load latest product data from GCS bucket.

    Returns:
        dict: Product records.
    """
    df = pd.read_csv(f"gs://meta-morph-flow/{today}/product_{today}.csv",
                     storage_options={
                            "token": SERVICE_KEY
                        }
                     ).set_index("Product Id")

    product_result = df.reset_index().to_dict(orient="records")
    return {"status": 200, "data": product_result}


# customers API to fetch the latest customer data
@app.get("/v1/customers")
async def load_customer_data(payload: dict = Depends(verify_token)):
    """
    API endpoint to load latest customer data from GCS bucket.
    Requires valid token for access.

    Parameters:
        payload (dict): Decoded JWT token payload (auto-injected by FastAPI).

    Returns:
        dict: Customer records.
    """
    df = pd.read_csv(
        f"gs://meta-morph-flow/{today}/customer_{today}.csv",
        storage_options={"token": SERVICE_KEY}
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
