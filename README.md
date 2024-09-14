# Spotify End-To-End Data Engineering Project

## Introduction
The project aims to extract the top 50 most-played songs globally from the Spotify app using the Spotipy API, transform the data within an AWS Lambda environment, and load it into AWS S3 in the desired format.

## Architecture
![Project Architecture Diagram](https://github.com/RemyPat/Spotify-ETL-project/blob/main/Spotify_Data_Pipeline.jpeg)

## Project Flow
1. Integrating Spotify API and extracting the data with Python.
2. Deploying the Extraction code within AWS Lambda for serverless execution.
3. Configure a scheduled trigger to automate the Lambda function execution.
4. Deploying the Tranformation and load code in AWS Lambda.
5. Enabling the Trigger for seemless automation of the ETL pipeline to store data in S3.
6. Constructing analytics tables on the transformed data using AWS Glue for data preparation and AWS Athena for querying and analysis.

## Technology used
- Programming Language- Python
- Amazon web Service(AWS)
  - AWS Lambda
  - S3 (Simple Storage Service)
  - Cloud Watch
  - Glue Crawler
  - Amazon Athena

## Dataset/API
Spotify API: https://developer.spotify.com/documentation/web-api

## Documentations used

- **Spotipy:** https://spotipy.readthedocs.io/en/2.24.0/#ids-uris-and-urls
- **spotipy.oauth2.SpotifyClientCredentials function:** https://snyk.io/advisor/python/spotipy/functions/spotipy.oauth2.SpotifyClientCredentials
- **Boto3:** https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html
