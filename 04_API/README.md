## Test the image locally

1. Build docker image

```shell
docker build .  -t api_server:latest
```

2. Run the image 

```shell
docker run  -e PORT=8080  -p '8080:8080' api_server:latest 
```

3. Check that the server is up, http://localhost:8080/status

## How to deploy to Google Cloud Run using CLI

1. Enable Google Cloud Run for the project: https://console.cloud.google.com/apis/library/run.googleapis.com

2. Enable the Container Registry API: https://console.cloud.google.com/marketplace/product/google/containerregistry.googleapis.com

3. Push the image
```shell
docker push gcr.io/psyched-freedom-376515/api-server:latest
```
4. Deploy the image
```shell
gcloud run deploy api-server --image gcr.io/psyched-freedom-376515/api-server:latest
```

5. To stop the server, we need to delete the service 
```shell
gcloud run services delete api-server --region=eu-west1
```