# About
Livepeer cloud storage upload utility. Called by Mist to upload video segments. Has CLI interface.

# Supported storage services
* File system
    * Doesn't require any parameters

# Behavior
- if upload operation succeeds, exits with return code 0 and reports URL in JSON format to `stdout`
- in case of error, return code is not zero, and error message is returned to stderr as plain text