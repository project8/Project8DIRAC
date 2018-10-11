## Requirements
In order to run a project8 dirac client you need:
- User certificate (see documents: )
- Make sure the location of the .globus is in the correct location 

## Instructions
To setup Project8Dirac client:
- download files in repo.
- docker-compose run --rm p8dirac_client

## Included files

* Dockerfile: instructs Docker software how to build the environment's disk image. We start from a basic CentOS 6 kernel and add just the most minimal additions required for functionality. Add packages as needed, following the format of a couple of included examples that are commented out by default.

* docker-composed.yaml: Setup the image and volumes used to run the DIRAC client

