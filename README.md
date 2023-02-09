# About
Livepeer cloud storage upload utility. Called by Mist to upload video segments. Has CLI interface.

# Supported storage services
* File system
* AWS S3
* Other S3-compatible storage
* Google Cloud Storage

# Behavior
- if upload operation succeeds, exits with return code 0 and reports URL in JSON format to `stdout`
- in case of error, return code is not zero, and error message is returned to stderr as plain text

# Example usage
```
./dms-uploader -uri s3://AWS_KEY:AWS_SECRET@eu-west-1/video-upload-test -key /test/fa7cb350-8978-4f7d-b54f-b0b67632fcf2.ts
```

# Running tests
Some tests require environment variables holding cloud service credentials to be set to run.