## 1. Creating Rest API using FastAPI : 
- Initially Create a virtual Environment `python -m venv myenv` (If it is not working try installing virtenv library)
- Switch to the Virtual Environment `myenv\Scripts\activate`
- Install the required libraries (for that create requirements.txt with all required libraries)
- Create code (main.py) here

## 2. Dockerise the App
- Create a file : Dockerfile
- Make sure that the contents are : 

    FROM python:3.8.18-slim

    WORKDIR /app

    COPY . /app

    RUN pip install -r requirements.txt

    CMD uvicorn main:app --port=8000 --host=0.0.0.0

From the above Lines : FROM indicates the Python version ; WORKDIR specifies the directory currently working on ; COPY . /app is like Copying (.) all from source to /app (working directory) ; RUN allows the python command to take over and install the libraries specified in requirements.txt file ; CMD is the command that helps running the uvicorn server at the specified port and host.

- Create a .dockerignore file to ignore the files that are not needed to be dockerised
- Create a file named docker-compose.yaml with the below content : 

    version: '3'

    services:
        web:
            build: .
            command: sh -c "uvicorn main:app --reload --port=8000 --host=0.0.0.0"
            env_file:
            - .env.development
            ports:
            - 8000:8000
            volumes:
            - .:/app

Name the services web. Use the current working directory where the docker file is created as a build context file. Write the command that runs the FastAPI App. Specify the Ports and Volumes

- Run the command `docker compose up --build`
- Similarly, Create the files for different environments.

## 3. Testing the Docker compose files
- In order to test the files, Let us create a .env file 
- Create a variable that stores a secret and then import to main.py
- Since there are 3 environments for us to test, We need 3 such .env files
- Open the Docker and sign in
- Compose it using the command : `docker compose -f docker-compose.development.yaml up --build`

## 4. Deploy Docker App in Cloud Run
- Open (Docker Web)[hub.docker.com] and create a repository
- Create a docker and build it like : `docker build -t yateeshchandra/rest-api:1.0.0 .`
- After build is completed, login to docker using : `docker login`
- Once Login succeeds, Push it to docker repo : `docker push yateeshchandra/rest-api:1.0.0`
- Now open Cloud Run in Google Cloud Console -> Deploy Container -> Service -> Repo Name