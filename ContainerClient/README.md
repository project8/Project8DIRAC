## Requirements
In order to run a project8 dirac client you need:
- User certificate (see documents: )
- Make sure that your local .globus directory located at ~/.globus 

## Instructions
To setup Project8Dirac client:
- download the files from this repo.
- execute `docker-compose run --rm p8dirac_client`

## Included files

* Dockerfile: instructs Docker software how to build the environment's disk image. The default only pulls the Project8DIRAC client container. You can add/build addition software as per the standard Docker methods.  
* docker-composed.yaml: Builds of the Dockerfile and maps required volumes (~/.globus:/root/.globus). You can add additional volumes as per the standard docker-compose methods.

