Docker 
====================

This folder provides with a copy of the infrastructure used in **DSSG** for running the *pipeline*.

Everything is implemented using `docker`. This *scripts* will create the environment *in* the machine in which the *script* is executed.

If you need to replicate the environment inside a Cloud Provider (for example **AWS** you can check this [link](https://medium.com/@lherrera/5-ways-of-using-docker-on-aws-7d91b31caddc#.ss5zwxktx).


Instructions
-------------------------

After cloning this repository, you need to provide *real* configuration files (see the main 
[`README`](https://github.com/dssg/cincinnati_ems/blob/master/README.md) of this repository), together with some valid keys
(named `id_rsa` and `ida_rsa.pub` for cloning this repository) in the folder `luigi-worker`.

Besides this you need to create a `.env` file using as example the file  `_env` 

After doing this you need to execute in the `infrastructure` folder the following:

```
docker-compose --project cincinnati up -d
```

If you need to stop the infrastructure

```
docker-compose --project cincinnati down
```

For scaling to 10 `luigi-worker`s:

```
docker-compose --project cincinnati scale luigi-worker=10
```


If you want to connect to some container (for example `luigi-worker`)

```
docker exec cincinnati-luigi-worker-1 /bin/bash
```
