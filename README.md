# About
Livepeer cloud storage upload utility. Called by Mist to upload video segments. Has CLI interface.

# Usage
```
  -h	Display usage information
  -id string
    	Storage service login or id
  -j	Describe supported storage services in JSON format and exit
  -l string
    	Log file path
  -param string
    	Additional storage service argument (e.g. AWS S3 region)
  -path string
    	File upload URI
  -secret string
    	Storage service password or secret
  -v int
    	Log verbosity, from 0 to 6: Panic, Fatal, Error, Warn, Info, Debug, Trace (default 4)
```

# Behavior
- if upload operation succeeds, exits with return code 0 and reports URL in JSON format to `stdout`
- in case of error, return code is not zero, and error message is returned to stderr as plain text